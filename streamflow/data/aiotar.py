import asyncio
import os
import shutil
from builtins import open as bltn_open
from tarfile import RECORDSIZE, TarFile


async def copyfileobj(src, dst, length=None, exception=OSError, bufsize=None):
    """Copy length bytes from fileobj src to fileobj dst.
       If length is None, copy the entire content.
    """
    bufsize = bufsize or 16 * 1024
    if length == 0:
        return
    if length is None:
        shutil.copyfileobj(src, dst, bufsize)
        return

    blocks, remainder = divmod(length, bufsize)
    for b in range(blocks):
        buf = src.read(bufsize)
        if len(buf) < bufsize:
            raise exception("unexpected end of data")
        dst.write(buf)
        await dst.drain()

    if remainder != 0:
        buf = src.read(remainder)
        if len(buf) < remainder:
            raise exception("unexpected end of data")
        dst.write(buf)
        await dst.drain()
    return


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
    async def add(self, name, arcname=None, recursive=True, *, filter=None):
        self._check("awx")
        if arcname is None:
            arcname = name
        # Skip if somebody tries to archive the archive...
        if self.name is not None and os.path.abspath(name) == self.name:
            self._dbg(2, "tarfile: Skipped %r" % name)
            return
        self._dbg(1, name)
        # Create a TarInfo object from the file.
        tarinfo = self.gettarinfo(name, arcname)
        if tarinfo is None:
            self._dbg(1, "tarfile: Unsupported type %r" % name)
            return
        # Change or exclude the TarInfo object.
        if filter is not None:
            tarinfo = filter(tarinfo)
            if tarinfo is None:
                self._dbg(2, "tarfile: Excluded %r" % name)
                return
        # Append the tar header and data to the archive.
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


open = AsyncTarFile.open
