from __future__ import annotations

import copy
import grp
import os
import pwd
import re
import shutil
import stat
import struct
import sys
import tarfile
import time
from abc import ABC
from builtins import open as bltn_open
from typing import Any, Optional

from streamflow.core.data import StreamWrapper
from streamflow.data.stream import BaseStreamWrapper


async def copyfileobj(src, dst, length=None, bufsize=None):
    bufsize = bufsize or 16 * 1024
    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst, bufsize)
        return
    blocks, remainder = divmod(length, bufsize)
    for b in range(blocks):
        await write(src, dst, bufsize)
    if remainder != 0:
        await write(src, dst, remainder)
    return


async def write(src, dst, bufsize):
    while bufsize > 0:
        buf = await src.read(bufsize) if isinstance(src, StreamWrapper) else src.read(bufsize)
        bufsize -= len(buf)
        await dst.write(buf) if isinstance(dst, StreamWrapper) else dst.write(buf)


class CompressionStreamWrapper(BaseStreamWrapper, ABC):

    def __init__(self, stream, mode):
        super().__init__(stream)
        self.mode = mode
        self.cmp = None
        self.exception = None

    async def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.mode == "w":
                await self.stream.write(self.cmp.flush())
        finally:
            await self.stream.close()

    async def read(self, size: Optional[int] = None):
        buf = await super().read(size)
        try:
            return self.cmp.decompress(buf)
        except self.exception:
            raise tarfile.ReadError("invalid compressed data")

    async def write(self, data: Any):
        data = self.cmp.compress(data)
        await super().write(data)


class BZ2StreamWrapper(CompressionStreamWrapper):

    def __init__(self, stream, mode, compresslevel: int = 9):
        super().__init__(stream, mode)
        try:
            import bz2
        except ImportError:
            raise tarfile.CompressionError("bz2 module is not available") from None
        if self.mode == "r":
            self.cmp = bz2.BZ2Decompressor()
            self.exception = OSError
        else:
            self.cmp = bz2.BZ2Compressor(compresslevel=compresslevel)


class GZipStreamWrapper(CompressionStreamWrapper):

    def __init__(self, stream, mode, compresslevel: int = 9):
        super().__init__(stream, mode)
        self.compresslevel: int = compresslevel
        self.cmp = None
        self.pos = 0
        try:
            import zlib
        except ImportError:
            raise tarfile.CompressionError("zlib module is not available") from None
        self.zlib = zlib
        self.crc = zlib.crc32(b"")
        if self.mode == "r":
            self.exception = zlib.error

    async def _init_compression(self):
        if self.mode == "r":
            await self._init_read_gz()
        else:
            await self._init_write_gz()

    async def _init_read_gz(self):
        self.cmp = self.zlib.decompressobj(-self.zlib.MAX_WBITS)
        if await self.stream.read(2) != b"\037\213":
            raise tarfile.ReadError("not a gzip file")
        if await self.stream.read(1) != b"\010":
            raise tarfile.CompressionError("unsupported compression method")
        flag = ord(await self.stream.read(1))
        await self.stream.read(6)
        if flag & 4:
            xlen = ord(await self.stream.read(1)) + 256 * ord(await self.stream.read(1))
            await self.stream.read(xlen)
        if flag & 8:
            while True:
                s = await self.stream.read(1)
                if not s or s == tarfile.NUL:
                    break
        if flag & 16:
            while True:
                s = await self.stream.read(1)
                if not s or s == tarfile.NUL:
                    break
        if flag & 2:
            await self.stream.read(2)

    async def _init_write_gz(self):
        self.cmp = self.zlib.compressobj(
            level=self.compresslevel,
            method=self.zlib.DEFLATED,
            wbits=-self.zlib.MAX_WBITS,
            memLevel=self.zlib.DEF_MEM_LEVEL,
            strategy=self.zlib.Z_DEFAULT_STRATEGY)
        timestamp = struct.pack("<L", int(time.time()))
        await self.stream.write(b"\037\213\010\010" + timestamp + b"\002\377" + tarfile.NUL)

    async def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            await self.stream.write(self.cmp.flush())
            await self.stream.write(struct.pack("<L", self.crc))
            await self.stream.write(struct.pack("<L", self.pos & 0xffffFFFF))
        finally:
            await self.stream.close()

    async def read(self, size: Optional[int] = None):
        if self.cmp is None:
            await self._init_compression()
        buf = await super().read(size)
        self.pos += len(buf)
        return buf

    async def write(self, data: Any):
        if self.cmp is None:
            await self._init_compression()
        self.crc = self.zlib.crc32(data, self.crc)
        self.pos += len(data)
        await super().write(data)


class LZMAStreamWrapper(CompressionStreamWrapper):

    def __init__(self, stream, mode, preset: Optional[int] = None):
        super().__init__(stream, mode)
        try:
            import lzma
        except ImportError:
            raise tarfile.CompressionError("lzma module is not available") from None
        if self.mode == "r":
            self.cmp = lzma.LZMADecompressor()
            self.exception = lzma.LZMAError
        else:
            self.cmp = lzma.LZMACompressor(preset=preset)


class FileStreamReaderWrapper(StreamWrapper):

    def __init__(self, stream, size, blockinfo=None):
        super().__init__(stream)
        self.size = size
        self.position = 0
        self.closed = False
        if blockinfo is None:
            blockinfo = [(0, size)]
        self.map_index = 0
        self.map = []
        lastpos = 0
        for offset, size in blockinfo:
            if offset > lastpos:
                self.map.append((False, lastpos, offset))
            self.map.append((True, offset, offset + size))
            lastpos = offset + size
        if lastpos < self.size:
            self.map.append((False, lastpos, self.size))

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()

    async def close(self):
        self.closed = True

    async def read(self, size: Optional[int] = None):
        size = self.size - self.position if size is None else min(size, self.size - self.position)
        while True:
            data, start, stop = self.map[self.map_index]
            if start <= self.position < stop:
                break
            else:
                self.map_index += 1
                if self.map_index == len(self.map):
                    return bytes()
        length = min(size, stop - self.position)
        if data:
            buf = await self.stream.read(length)
            self.position += len(buf)
            return buf
        else:
            self.position += length
            return tarfile.NUL * length

    async def write(self, data: Any):
        raise NotImplementedError


class AioTarInfo(tarfile.TarInfo):

    @classmethod
    async def fromtarfile(cls, tarstream):
        buf = await tarstream.stream.read(tarfile.BLOCKSIZE)
        obj = cls.frombuf(buf, tarstream.encoding, tarstream.errors)
        obj.offset = tarstream.offset - tarfile.BLOCKSIZE
        return await obj._proc_member(tarstream)

    def _proc_builtin(self, tarstream):
        self.offset_data = tarstream.offset
        offset = self.offset_data
        if self.isreg() or self.type not in tarfile.SUPPORTED_TYPES:
            offset += self._block(self.size)
        tarstream.offset = offset
        self._apply_pax_info(tarstream.pax_headers, tarstream.encoding, tarstream.errors)
        return self

    async def _proc_gnulong(self, tarstream):
        buf = await tarstream.stream.read(self._block(self.size))
        try:
            next = await self.fromtarfile(tarstream)
        except tarfile.HeaderError as e:
            raise tarfile.SubsequentHeaderError(str(e)) from None
        next.offset = self.offset
        if self.type == tarfile.GNUTYPE_LONGNAME:
            next.name = tarfile.nts(buf, tarstream.encoding, tarstream.errors)
        elif self.type == tarfile.GNUTYPE_LONGLINK:
            next.linkname = tarfile.nts(buf, tarstream.encoding, tarstream.errors)
        return next

    async def _proc_gnusparse_10(self, next, tarstream):
        sparse = []
        buf = await tarstream.stream.read(tarfile.BLOCKSIZE)
        fields, buf = buf.split(b"\n", 1)
        fields = int(fields)
        while len(sparse) < fields * 2:
            if b"\n" not in buf:
                buf += await tarstream.stream.read(tarfile.BLOCKSIZE)
            number, buf = buf.split(b"\n", 1)
            sparse.append(int(number))
        next.offset_data = tarstream.offset
        next.sparse = list(zip(sparse[::2], sparse[1::2]))

    async def _proc_member(self, tarstream):
        if self.type in (tarfile.GNUTYPE_LONGNAME, tarfile.GNUTYPE_LONGLINK):
            return await self._proc_gnulong(tarstream)
        elif self.type == tarfile.GNUTYPE_SPARSE:
            return await self._proc_sparse(tarstream)
        elif self.type in (tarfile.XHDTYPE, tarfile.XGLTYPE, tarfile.SOLARIS_XHDTYPE):
            return await self._proc_pax(tarstream)
        else:
            return self._proc_builtin(tarstream)

    async def _proc_pax(self, tarstream):
        buf = await tarstream.stream.read(self._block(self.size))
        if self.type == tarfile.XGLTYPE:
            pax_headers = tarstream.pax_headers
        else:
            pax_headers = tarstream.pax_headers.copy()
        match = re.search(br"\d+ hdrcharset=([^\n]+)\n", buf)
        if match is not None:
            pax_headers["hdrcharset"] = match.group(1).decode("utf-8")
        hdrcharset = pax_headers.get("hdrcharset")
        if hdrcharset == "BINARY":
            encoding = tarstream.encoding
        else:
            encoding = "utf-8"
        regex = re.compile(br"(\d+) ([^=]+)=")
        pos = 0
        while True:
            match = regex.match(buf, pos)
            if not match:
                break
            length, keyword = match.groups()
            length = int(length)
            if length == 0:
                raise tarfile.InvalidHeaderError("invalid header")
            value = buf[match.end(2) + 1:match.start(1) + length - 1]
            keyword = self._decode_pax_field(keyword, "utf-8", "utf-8", tarstream.errors)
            if keyword in tarfile.PAX_NAME_FIELDS:
                value = self._decode_pax_field(value, encoding, tarstream.encoding, tarstream.errors)
            else:
                value = self._decode_pax_field(value, "utf-8", "utf-8", tarstream.errors)
            pax_headers[keyword] = value
            pos += length
        try:
            next = await self.fromtarfile(tarstream)
        except tarfile.HeaderError as e:
            raise tarfile.SubsequentHeaderError(str(e)) from None
        if "GNU.sparse.map" in pax_headers:
            self._proc_gnusparse_01(next, pax_headers)
        elif "GNU.sparse.size" in pax_headers:
            self._proc_gnusparse_00(next, pax_headers, buf)
        elif pax_headers.get("GNU.sparse.major") == "1" and pax_headers.get("GNU.sparse.minor") == "0":
            await self._proc_gnusparse_10(next, tarstream)
        if self.type in (tarfile.XHDTYPE, tarfile.SOLARIS_XHDTYPE):
            next._apply_pax_info(pax_headers, tarstream.encoding, tarstream.errors)
            next.offset = self.offset
            if "size" in pax_headers:
                offset = next.offset_data
                if next.isreg() or next.type not in tarfile.SUPPORTED_TYPES:
                    offset += next._block(next.size)
                tarstream.offset = offset
        return next

    async def _proc_sparse(self, tarstream):
        structs, isextended, origsize = self._sparse_structs
        del self._sparse_structs
        while isextended:
            buf = await tarstream.stream.read(tarfile.BLOCKSIZE)
            pos = 0
            for i in range(21):
                try:
                    offset = tarfile.nti(buf[pos:pos + 12])
                    numbytes = tarfile.nti(buf[pos + 12:pos + 24])
                except ValueError:
                    break
                if offset and numbytes:
                    structs.append((offset, numbytes))
                pos += 24
            isextended = bool(buf[504])
        self.sparse = structs
        self.offset_data = tarstream.offset
        tarstream.offset = self.offset_data + self._block(self.size)
        self.size = origsize
        return self


class AioTarStream(object):
    debug = 0
    dereference = False
    ignore_zeros = False
    errorlevel = 1
    format = tarfile.DEFAULT_FORMAT
    encoding = tarfile.ENCODING
    errors = None
    tarinfo = AioTarInfo
    fileobject = FileStreamReaderWrapper

    def __init__(self,
                 stream,
                 mode="r",
                 format=None,
                 tarinfo=None,
                 dereference=None,
                 ignore_zeros=None,
                 encoding=None,
                 errors="surrogateescape",
                 pax_headers=None,
                 debug=None,
                 errorlevel=None,
                 copybufsize=None):
        modes = {"r": "rb", "a": "r+b", "w": "wb", "x": "xb"}
        if mode not in modes:
            raise ValueError("mode must be 'r', 'a', 'w' or 'x'")
        self.mode = mode
        self._mode = stream.mode if hasattr(stream, "mode") else modes[mode]
        self.stream = stream
        self.offset = 0
        self.index = 0
        if format is not None:
            self.format = format
        if tarinfo is not None:
            self.tarinfo = tarinfo
        if dereference is not None:
            self.dereference = dereference
        if ignore_zeros is not None:
            self.ignore_zeros = ignore_zeros
        if encoding is not None:
            self.encoding = encoding
        self.errors = errors
        self.pax_headers = pax_headers if pax_headers is not None and self.format == tarfile.PAX_FORMAT else {}
        if debug is not None:
            self.debug = debug
        if errorlevel is not None:
            self.errorlevel = errorlevel
        self.copybufsize = copybufsize
        self.closed = False
        self.members = []
        self._loaded = False
        self.inodes = {}

    async def __aenter__(self):
        self._check()
        try:
            if self.mode == "r":
                self.firstmember = None
                self.firstmember = await self.next()
            if self.mode in ("a", "w", "x"):
                self._loaded = True
                if self.pax_headers:
                    buf = self.tarinfo.create_pax_global_header(self.pax_headers.copy())
                    await self.stream.write(buf)
                    self.offset += len(buf)
            return self
        except BaseException:
            self.closed = True
            raise

    async def __aexit__(self, exc_type, value, traceback):
        if exc_type is None:
            await self.close()
        else:
            self.closed = True

    def __aiter__(self):
        return self

    async def __anext__(self):
        index = self.index
        self.index += 1
        if self._loaded:
            if index < len(self.members):
                return self.members[index]
        else:
            if index == 0 and self.firstmember is not None:
                return await self.next()
            elif index < len(self.members):
                return self.members[index]
            else:
                tarinfo = await self.next()
                if not tarinfo:
                    self._loaded = True
                    raise StopAsyncIteration
                else:
                    return tarinfo

    @classmethod
    def open(cls, stream, mode="r", **kwargs):
        filemode, comptype = mode.split(":", 1) if ':' in mode else (mode, None)
        filemode = mode or "r"
        comptype = comptype or "tar"
        if filemode not in ("r", "w"):
            raise ValueError("mode must be 'r' or 'w'")
        if comptype == "gz":
            stream = GZipStreamWrapper(stream, filemode)
        elif comptype == "bz2":
            stream = BZ2StreamWrapper(stream, filemode)
        elif comptype == "xz":
            stream = LZMAStreamWrapper(stream, filemode)
        elif comptype != "tar":
            raise tarfile.CompressionError("unknown compression type %r" % comptype)
        else:
            stream = BaseStreamWrapper(stream)
        t = cls(stream, filemode, **kwargs)
        return t

    @classmethod
    def taropen(cls, stream, mode="r", **kwargs):
        if mode not in ("r", "a", "w", "x"):
            raise ValueError("mode must be 'r', 'a', 'w' or 'x'")
        return cls(BaseStreamWrapper(stream), mode, **kwargs)

    @classmethod
    def gzopen(cls, stream, mode="r", compresslevel=9, **kwargs):
        if mode not in ("r", "w", "x"):
            raise ValueError("mode must be 'r', 'w' or 'x'")
        return cls(GZipStreamWrapper(stream, mode, compresslevel), mode, **kwargs)

    @classmethod
    def bz2open(cls, stream, mode="r", compresslevel=9, **kwargs):
        if mode not in ("r", "w", "x"):
            raise ValueError("mode must be 'r', 'w' or 'x'")
        return cls(BZ2StreamWrapper(stream, mode, compresslevel), mode, **kwargs)

    @classmethod
    def xzopen(cls, stream, mode="r", preset=None, **kwargs):
        if mode not in ("r", "w", "x"):
            raise ValueError("mode must be 'r', 'w' or 'x'")
        return cls(LZMAStreamWrapper(stream, mode, preset), mode, **kwargs)

    def _check(self, mode=None):
        if self.closed:
            raise OSError("%s is closed" % self.__class__.__name__)
        if mode is not None and self.mode not in mode:
            raise OSError("bad operation for mode %r" % self.mode)

    def _dbg(self, level, msg):
        if level <= self.debug:
            print(msg, file=sys.stderr)

    async def _extract_member(self, tarinfo, targetpath, set_attrs=True, numeric_owner=False):
        targetpath = targetpath.rstrip("/")
        targetpath = targetpath.replace("/", os.sep)
        upperdirs = os.path.dirname(targetpath)
        if upperdirs and not os.path.exists(upperdirs):
            os.makedirs(upperdirs)
        if tarinfo.islnk() or tarinfo.issym():
            self._dbg(1, "%s -> %s" % (tarinfo.name, tarinfo.linkname))
        else:
            self._dbg(1, tarinfo.name)
        if tarinfo.isreg():
            await self.makefile(tarinfo, targetpath)
        elif tarinfo.isdir():
            self.makedir(tarinfo, targetpath)
        elif tarinfo.isfifo():
            self.makefifo(tarinfo, targetpath)
        elif tarinfo.ischr() or tarinfo.isblk():
            self.makedev(tarinfo, targetpath)
        elif tarinfo.islnk() or tarinfo.issym():
            await self.makelink(tarinfo, targetpath)
        elif tarinfo.type not in tarfile.SUPPORTED_TYPES:
            await self.makeunknown(tarinfo, targetpath)
        else:
            await self.makefile(tarinfo, targetpath)
        if set_attrs:
            self.chown(tarinfo, targetpath, numeric_owner)
            if not tarinfo.issym():
                self.chmod(tarinfo, targetpath)
                self.utime(tarinfo, targetpath)

    async def _find_link_target(self, tarinfo):
        if tarinfo.issym():
            linkname = "/".join(filter(None, (os.path.dirname(tarinfo.name), tarinfo.linkname)))
            limit = None
        else:
            linkname = tarinfo.linkname
            limit = tarinfo
        member = await self._getmember(linkname, tarinfo=limit, normalize=True)
        if member is None:
            raise KeyError("linkname %r not found" % linkname)
        return member

    async def _getmember(self, name, tarinfo=None, normalize=False):
        members = await self.getmembers()
        if tarinfo is not None:
            members = members[:members.index(tarinfo)]
        if normalize:
            name = os.path.normpath(name)
        for member in reversed(members):
            if normalize:
                member_name = os.path.normpath(member.name)
            else:
                member_name = member.name
            if name == member_name:
                return member

    async def _load(self):
        while True:
            tarinfo = await self.next()
            if tarinfo is None:
                break
        self._loaded = True

    async def add(self, name, arcname=None, recursive=True, *, filter=None):
        self._check("awx")
        if arcname is None:
            arcname = name
        self._dbg(1, name)
        tarinfo = self.gettarinfo(name, arcname)
        if tarinfo is None:
            self._dbg(1, "tarfile: Unsupported type %r" % name)
            return
        if filter is not None:
            tarinfo = filter(tarinfo)
            if tarinfo is None:
                self._dbg(2, "tarfile: Excluded %r" % name)
                return
        if tarinfo.isreg():
            with bltn_open(name, "rb") as f:
                await self.addfile(tarinfo, f)
        elif tarinfo.isdir():
            await self.addfile(tarinfo)
            if recursive:
                for f in sorted(os.listdir(name)):
                    await self.add(os.path.join(name, f), os.path.join(arcname, f), recursive, filter=filter)
        else:
            await self.addfile(tarinfo)

    async def addfile(self, tarinfo, fileobj=None):
        self._check("awx")
        tarinfo = copy.copy(tarinfo)
        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        await self.stream.write(buf)
        self.offset += len(buf)
        bufsize = self.copybufsize
        if fileobj is not None:
            await copyfileobj(fileobj, self.stream, tarinfo.size, bufsize)
            blocks, remainder = divmod(tarinfo.size, tarfile.BLOCKSIZE)
            if remainder > 0:
                await self.stream.write(tarfile.NUL * (tarfile.BLOCKSIZE - remainder))
                blocks += 1
            self.offset += blocks * tarfile.BLOCKSIZE
        self.members.append(tarinfo)

    def chown(self, tarinfo, targetpath, numeric_owner):
        if hasattr(os, "geteuid") and os.geteuid() == 0:
            g = tarinfo.gid
            u = tarinfo.uid
            if not numeric_owner:
                try:
                    if grp:
                        g = grp.getgrnam(tarinfo.gname)[2]
                except KeyError:
                    pass
                try:
                    if pwd:
                        u = pwd.getpwnam(tarinfo.uname)[2]
                except KeyError:
                    pass
            try:
                if tarinfo.issym() and hasattr(os, "lchown"):
                    os.lchown(targetpath, u, g)
                else:
                    os.chown(targetpath, u, g)
            except OSError as e:
                raise tarfile.ExtractError("could not change owner") from e

    def chmod(self, tarinfo, targetpath):
        try:
            os.chmod(targetpath, tarinfo.mode)
        except OSError as e:
            raise tarfile.ExtractError("could not change mode") from e

    async def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            if self.mode in ("a", "w", "x"):
                await self.stream.write(tarfile.NUL * (tarfile.BLOCKSIZE * 2))
                self.offset += (tarfile.BLOCKSIZE * 2)
                blocks, remainder = divmod(self.offset, tarfile.RECORDSIZE)
                if remainder > 0:
                    await self.stream.write(tarfile.NUL * (tarfile.RECORDSIZE - remainder))
        finally:
            await self.stream.close()

    async def extract(self, member, path="", set_attrs=True, *, numeric_owner=False):
        self._check("r")
        tarinfo = await self.getmember(member) if isinstance(member, str) else member
        if tarinfo.islnk():
            tarinfo._link_target = os.path.join(path, tarinfo.linkname)
        try:
            await self._extract_member(tarinfo,
                                       os.path.join(path, tarinfo.name),
                                       set_attrs=set_attrs,
                                       numeric_owner=numeric_owner)
        except OSError as e:
            if self.errorlevel > 0:
                raise
            else:
                if e.filename is None:
                    self._dbg(1, "tarfile: %s" % e.strerror)
                else:
                    self._dbg(1, "tarfile: %s %r" % (e.strerror, e.filename))
        except tarfile.ExtractError as e:
            if self.errorlevel > 1:
                raise
            else:
                self._dbg(1, "tarfile: %s" % e)

    async def extractall(self, path=".", members=None, *, numeric_owner=False):
        directories = []
        if members is None:
            members = self
        for tarinfo in members:
            if tarinfo.isdir():
                directories.append(tarinfo)
                tarinfo = copy.copy(tarinfo)
                tarinfo.mode = 0o700
            await self.extract(tarinfo, path, set_attrs=not tarinfo.isdir(), numeric_owner=numeric_owner)
        directories.sort(key=lambda a: a.name)
        directories.reverse()
        for tarinfo in directories:
            dirpath = os.path.join(path, tarinfo.name)
            try:
                self.chown(tarinfo, dirpath, numeric_owner=numeric_owner)
                self.utime(tarinfo, dirpath)
                self.chmod(tarinfo, dirpath)
            except tarfile.ExtractError as e:
                if self.errorlevel > 1:
                    raise
                else:
                    self._dbg(1, "tarfile: %s" % e)

    async def extractfile(self, member):
        self._check("r")
        tarinfo = await self.getmember(member) if isinstance(member, str) else member
        if tarinfo.isreg() or tarinfo.type not in tarinfo.SUPPORTED_TYPES:
            return self.fileobject(
                stream=self.stream,
                size=tarinfo.size,
                blockinfo=tarinfo.sparse)
        elif tarinfo.islnk() or tarinfo.issym():
            raise tarinfo.StreamError("cannot extract (sym)link as file object")
        else:
            return None

    async def getmember(self, name):
        tarinfo = await self._getmember(name.rstrip('/'))
        if tarinfo is None:
            raise KeyError("filename %r not found" % name)
        return tarinfo

    async def getmembers(self):
        self._check()
        if not self._loaded:
            await self._load()
        return self.members

    async def getnames(self):
        return [tarinfo.name for tarinfo in await self.getmembers()]

    def gettarinfo(self, name=None, arcname=None, fileobj=None):
        self._check("awx")
        if fileobj is not None:
            name = fileobj.name
        if arcname is None:
            arcname = name
        drv, arcname = os.path.splitdrive(arcname)
        arcname = arcname.replace(os.sep, "/")
        arcname = arcname.lstrip("/")
        tarinfo = self.tarinfo()
        if fileobj is None:
            statres = os.lstat(name) if not self.dereference else os.stat(name)
        else:
            statres = os.fstat(fileobj.fileno())
        linkname = ""
        stmd = statres.st_mode
        if stat.S_ISREG(stmd):
            inode = (statres.st_ino, statres.st_dev)
            if not self.dereference and statres.st_nlink > 1 and \
                    inode in self.inodes and arcname != self.inodes[inode]:
                type = tarfile.LNKTYPE
                linkname = self.inodes[inode]
            else:
                type = tarfile.REGTYPE
                if inode[0]:
                    self.inodes[inode] = arcname
        elif stat.S_ISDIR(stmd):
            type = tarfile.DIRTYPE
        elif stat.S_ISFIFO(stmd):
            type = tarfile.FIFOTYPE
        elif stat.S_ISLNK(stmd):
            type = tarfile.SYMTYPE
            linkname = os.readlink(name)
        elif stat.S_ISCHR(stmd):
            type = tarfile.CHRTYPE
        elif stat.S_ISBLK(stmd):
            type = tarfile.BLKTYPE
        else:
            return None
        tarinfo.name = arcname
        tarinfo.mode = stmd
        tarinfo.uid = statres.st_uid
        tarinfo.gid = statres.st_gid
        tarinfo.size = statres.st_size if type == tarfile.REGTYPE else 0
        tarinfo.mtime = statres.st_mtime
        tarinfo.type = type
        tarinfo.linkname = linkname
        if pwd:
            try:
                tarinfo.uname = pwd.getpwuid(tarinfo.uid)[0]
            except KeyError:
                pass
        if grp:
            try:
                tarinfo.gname = grp.getgrgid(tarinfo.gid)[0]
            except KeyError:
                pass

        if type in (tarfile.CHRTYPE, tarfile.BLKTYPE):
            if hasattr(os, "major") and hasattr(os, "minor"):
                tarinfo.devmajor = os.major(statres.st_rdev)
                tarinfo.devminor = os.minor(statres.st_rdev)
        return tarinfo

    def list(self, verbose=True, *, members=None):
        self._check()
        if members is None:
            members = self
        for tarinfo in members:
            if verbose:
                tarfile._safe_print(stat.filemode(tarinfo.mode))
                tarfile._safe_print("%s/%s" % (tarinfo.uname or tarinfo.uid, tarinfo.gname or tarinfo.gid))
                if tarinfo.ischr() or tarinfo.isblk():
                    tarfile._safe_print("%10s" % ("%d,%d" % (tarinfo.devmajor, tarinfo.devminor)))
                else:
                    tarfile._safe_print("%10d" % tarinfo.size)
                tarfile._safe_print("%d-%02d-%02d %02d:%02d:%02d" % time.localtime(tarinfo.mtime)[:6])
            tarfile._safe_print(tarinfo.name + ("/" if tarinfo.isdir() else ""))
            if verbose:
                if tarinfo.issym():
                    tarfile._safe_print("-> " + tarinfo.linkname)
                if tarinfo.islnk():
                    tarfile._safe_print("link to " + tarinfo.linkname)
            print()

    def makedev(self, tarinfo, targetpath):
        if not hasattr(os, "mknod") or not hasattr(os, "makedev"):
            raise tarfile.ExtractError("special devices not supported by system")
        mode = tarinfo.mode
        if tarinfo.isblk():
            mode |= stat.S_IFBLK
        else:
            mode |= stat.S_IFCHR
        os.mknod(targetpath, mode, os.makedev(tarinfo.devmajor, tarinfo.devminor))

    def makedir(self, tarinfo, targetpath):
        try:
            os.mkdir(targetpath, 0o700)
        except FileExistsError:
            pass

    def makefifo(self, tarinfo, targetpath):
        if hasattr(os, "mkfifo"):
            os.mkfifo(targetpath)
        else:
            raise tarfile.ExtractError("fifo not supported by system")

    async def makefile(self, tarinfo, targetpath):
        bufsize = self.copybufsize
        with bltn_open(targetpath, "wb") as target:
            if tarinfo.sparse is not None:
                for offset, size in tarinfo.sparse:
                    target.seek(offset)
                    await copyfileobj(self.stream, target, size, bufsize)
                target.seek(tarinfo.size)
                target.truncate()
            else:
                await copyfileobj(self.stream, target, tarinfo.size, bufsize)

    async def makelink(self, tarinfo, targetpath):
        try:
            if tarinfo.issym():
                if os.path.lexists(targetpath):
                    os.unlink(targetpath)
                os.symlink(tarinfo.linkname, targetpath)
            else:
                if os.path.exists(tarinfo._link_target):
                    os.link(tarinfo._link_target, targetpath)
                else:
                    await self._extract_member(await self._find_link_target(tarinfo), targetpath)
        except tarfile.symlink_exception:
            try:
                await self._extract_member(await self._find_link_target(tarinfo), targetpath)
            except KeyError:
                raise tarfile.ExtractError("unable to resolve link inside archive") from None

    async def makeunknown(self, tarinfo, targetpath):
        await self.makefile(tarinfo, targetpath)
        self._dbg(1, "tarfile: Unknown file type %r, extracted as regular file." % tarinfo.type)

    async def next(self):
        self._check("ra")
        if self.firstmember is not None:
            m = self.firstmember
            self.firstmember = None
            return m
        tarinfo = None
        while True:
            try:
                tarinfo = await self.tarinfo.fromtarfile(self)
            except tarfile.EOFHeaderError as e:
                if self.ignore_zeros:
                    self._dbg(2, "0x%X: %s" % (self.offset, e))
                    self.offset += tarfile.BLOCKSIZE
                    continue
            except tarfile.InvalidHeaderError as e:
                if self.ignore_zeros:
                    self._dbg(2, "0x%X: %s" % (self.offset, e))
                    self.offset += tarfile.BLOCKSIZE
                    continue
                elif self.offset == 0:
                    raise tarfile.ReadError(str(e)) from None
            except tarfile.EmptyHeaderError:
                if self.offset == 0:
                    raise tarfile.ReadError("empty file") from None
            except tarfile.TruncatedHeaderError as e:
                if self.offset == 0:
                    raise tarfile.ReadError(str(e)) from None
            except tarfile.SubsequentHeaderError as e:
                raise tarfile.ReadError(str(e)) from None
            except Exception as e:
                try:
                    import zlib
                    if isinstance(e, zlib.error):
                        raise tarfile.ReadError(f'zlib error: {e}') from None
                    else:
                        raise e
                except ImportError:
                    raise e
            break
        if tarinfo is not None:
            self.members.append(tarinfo)
        else:
            self._loaded = True
        return tarinfo

    def utime(self, tarinfo, targetpath):
        if not hasattr(os, 'utime'):
            return
        try:
            os.utime(targetpath, (tarinfo.mtime, tarinfo.mtime))
        except OSError as e:
            raise tarfile.ExtractError("could not change modification time") from e


open = AioTarStream.open
