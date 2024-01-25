import unittest

import aiotarfile
from aiofiles.tempfile import TemporaryFile

class TestBasic(unittest.IsolatedAsyncioTestCase):
    async def test_basic(self):
        async with TemporaryFile() as fp, TemporaryFile() as fp2:
            await fp2.write(b'hello world')
            await fp2.seek(0)

            async with aiotarfile.open_wr(fp) as tar:
                await tar.add_file('test', 0o755, fp2)
                await tar.add_dir('dir', 0o755)
                await tar.add_symlink('dir/test', 0o777, '../test')

            await fp.seek(0)
            data = await fp.read()
            assert b'hello world' in data

            await fp.seek(0)
            async with aiotarfile.open_rd(fp) as tar:
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
