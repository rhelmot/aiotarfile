import unittest
import io

import aiotarfile

class AsyncBytesIO:
    def __init__(self, data=b''):
        self.inner = io.BytesIO(data)

    async def read(self, n=-1) -> bytes:
        return self.inner.read(n)

    async def write(self, buf: bytes) -> int:
        return self.inner.write(buf)

    async def close(self) -> None:
        pass

    async def seek(self, *args):
        return self.inner.seek(*args)

    async def flush(self):
        pass

class TestBasic(unittest.IsolatedAsyncioTestCase):
    async def test_basic(self):
        for compression in [
            aiotarfile.CompressionType.Clear,
            aiotarfile.CompressionType.Gzip,
            aiotarfile.CompressionType.Bzip2,
            aiotarfile.CompressionType.Xz,
        ]:
            await self.inner_basic(compression)

    async def inner_basic(self, compression: aiotarfile.CompressionType):
        fp = AsyncBytesIO()
        fp2 = AsyncBytesIO()

        await fp2.write(b'hello world')
        await fp2.seek(0)

        async with await aiotarfile.open_wr(fp, compression) as tar:
            await tar.add_file('test', 0o755, fp2)
            await tar.add_dir('dir', 0o755)
            await tar.add_symlink('dir/test', 0o777, '../test')

        await fp.seek(0)
        data = await fp.read()

        for xcompression in [compression, aiotarfile.CompressionType.Detect]:
            await fp.seek(0)
            async with await aiotarfile.open_rd(fp, xcompression) as tar:
                seen = set()
                async for entry in tar:
                    assert entry.name() not in seen
                    seen.add(entry.name())
                    if entry.name() == b'test':
                        assert entry.entry_type() == aiotarfile.TarfileEntryType.Regular
                        assert entry.mode() == 0o755
                        assert await entry.read() == b'hello world'
                    elif entry.name() == b'dir':
                        assert entry.entry_type() == aiotarfile.TarfileEntryType.Directory
                        assert entry.mode() == 0o755
                    elif entry.name() == b'dir/test':
                        assert entry.entry_type() == aiotarfile.TarfileEntryType.Symlink
                        assert entry.mode() == 0o777
                        assert entry.link_target() == b'../test'
                    else:
                        raise Exception("Unexpected entry", entry.name())

if __name__ == '__main__':
    unittest.main()
