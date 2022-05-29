import asyncio
import copy
import inspect
import os
import shutil
from asyncio import StreamWriter
from builtins import open as bltn_open
from tarfile import BLOCKSIZE, ExtractError, NUL, RECORDSIZE, ReadError, SUPPORTED_TYPES, TarFile
from typing import cast


async def copyfileobj(src, dst, length=None, exception=OSError, bufsize=None):
    bufsize = bufsize or 16 * 1024
    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst, bufsize)
        return
    blocks, remainder = divmod(length, bufsize)
    for b in range(blocks):
        await write(src, dst, bufsize, exception)
    if remainder != 0:
        await write(src, dst, remainder, exception)
    return


async def write(src, dst, bufsize, exception):
    buf = await src.read(bufsize) if inspect.iscoroutinefunction(src.read) else src.read(bufsize)
    if len(buf) < bufsize:
        raise exception("unexpected end of data")
    dst.write(buf)
    if hasattr(dst, 'drain'):
        await dst.drain()


class AsyncTarFile(TarFile):

    @classmethod
    def open(cls, name=None, mode="r", fileobj=None, bufsize=RECORDSIZE, **kwargs):
        if not name and not fileobj:
            raise ValueError("nothing to open")
        if "|" in mode:
            t = super().open(name, mode, fileobj, bufsize, **kwargs)

            async def drain(self):
                await self.fileobj.drain()

            type(t.fileobj).drain = drain
            return t
        else:
            return super().open(name, mode, fileobj, bufsize, **kwargs)

    # noinspection PyUnresolvedReferences
    async def _extract_member(self, tarinfo, targetpath, set_attrs=True,
                              numeric_owner=False):
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
            self.makelink(tarinfo, targetpath)
        elif tarinfo.type not in SUPPORTED_TYPES:
            await self.makeunknown(tarinfo, targetpath)
        else:
            await self.makefile(tarinfo, targetpath)
        if set_attrs:
            self.chown(tarinfo, targetpath, numeric_owner)
            if not tarinfo.issym():
                self.chmod(tarinfo, targetpath)
                self.utime(tarinfo, targetpath)

    # noinspection PyUnresolvedReferences
    async def add(self, name, arcname=None, recursive=True, *, filter=None):
        self._check("awx")
        if arcname is None:
            arcname = name
        if self.name is not None and os.path.abspath(name) == self.name:
            self._dbg(2, "tarfile: Skipped %r" % name)
            return
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
                await asyncio.gather(*(asyncio.create_task(
                    self.add(
                        os.path.join(name, f),
                        os.path.join(arcname, f),
                        recursive,
                        filter=filter))
                    for f in sorted(os.listdir(name))))
        else:
            await self.addfile(tarinfo)

    # noinspection PyUnresolvedReferences
    async def addfile(self, tarinfo, fileobj=None):
        self._check("awx")
        tarinfo = copy.copy(tarinfo)
        buf = tarinfo.tobuf(self.format, self.encoding, self.errors)
        self.fileobj.write(buf)
        self.offset += len(buf)
        bufsize = self.copybufsize
        # If there's data to follow, append it.
        if fileobj is not None:
            await copyfileobj(fileobj, cast(StreamWriter, self.fileobj), tarinfo.size, bufsize=bufsize)
            blocks, remainder = divmod(tarinfo.size, BLOCKSIZE)
            if remainder > 0:
                self.fileobj.write(NUL * (BLOCKSIZE - remainder))
                await self.fileobj.drain()
                blocks += 1
            self.offset += blocks * BLOCKSIZE
        self.members.append(tarinfo)

    # noinspection PyUnresolvedReferences
    async def extract(self, member, path="", set_attrs=True, *, numeric_owner=False):
        self._check("r")
        if isinstance(member, str):
            tarinfo = self.getmember(member)
        else:
            tarinfo = member
        if tarinfo.islnk():
            tarinfo._link_target = os.path.join(path, tarinfo.linkname)
        try:
            await self._extract_member(tarinfo, os.path.join(path, tarinfo.name),
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
        except ExtractError as e:
            if self.errorlevel > 1:
                raise
            else:
                self._dbg(1, "tarfile: %s" % e)

    # noinspection PyUnresolvedReferences
    async def makefile(self, tarinfo, targetpath):
        source = self.fileobj
        source.seek(tarinfo.offset_data)
        bufsize = self.copybufsize
        with bltn_open(targetpath, "wb") as target:
            if tarinfo.sparse is not None:
                for offset, size in tarinfo.sparse:
                    target.seek(offset)
                    await copyfileobj(source, target, size, ReadError, bufsize)
                target.seek(tarinfo.size)
                target.truncate()
            else:
                await copyfileobj(source, target, tarinfo.size, ReadError, bufsize)

    # noinspection PyUnresolvedReferences
    async def makeunknown(self, tarinfo, targetpath):
        await self.makefile(tarinfo, targetpath)
        self._dbg(1, "tarfile: Unknown file type %r, extracted as regular file." % tarinfo.type)


open = AsyncTarFile.open
