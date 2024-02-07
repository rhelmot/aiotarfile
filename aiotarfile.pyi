"""
A module for asynchronous access to reading and writing streaming tarballs.

Your entry points should be `open_rd` and `open_wr`.
"""
from typing import Protocol, Any

class AWriteStream(Protocol):
    async def write(self, data: bytes | bytearray | memoryview, /) -> int: ...
    async def close(self) -> Any: ...

class AReadStream(Protocol):
    async def read(self, n: int = -1, /) -> bytes: ...

async def open_rd(fp: AReadStream, compression: CompressionType = CompressionType.Detect) -> TarfileRd:
    """
    Open a tar file for reading.

    This function takes an asynchronous stream, i.e. an object with `async def read(self, n=-1) -> bytes`
    It returns a `TarfileRd` object.
    """
async def open_wr(fp: AWriteStream, compression: CompressionType = CompressionType.Clear) -> TarfileWr:
    """
    Open a tar file for writing.
    
    This function takes an asynchronous stream, i.e. an object with `async def write(self, buf: bytes) -> int`
    and `async def close(self)`
    It returns a `TarfileWr` object.
    """

class TarfileWr:
    """
    The main tar builder object.

    Do not construct this class manually, instead use `open_wr` on the module.
    """
    async def add_file(self, name: str | bytes, mode: int, content: AReadStream, size: int | None = None):
        """
        Add a regular file to the archive.
        
        `content` should be an asynchronous stream, i.e. and object with
        `async def read(self, n=-1) -> bytes`.
        
        Warning: if `size` is not provided, the entire contents of the input stream will be
        buffered into memory in order to count its size.
        """
    async def add_dir(self, name: str | bytes, mode: int):
        """
        Add a directory to the archive.
        """
    async def add_symlink(self, name: str | bytes, mode: int, target: str | bytes):
        """
        Add a symlink to the archive.
        """
    async def close(self):
        """
        Close the archive.
        
        This operation finalizes and flushes the output stream and then renders the
        object useless for future operations.
        """
    async def __aenter__(self) -> TarfileWr:
        """
        Open the archive in a context manager.
        
        `TarfileWr` may be used in an `async with` block. This will cause `close()` to be
        automatically called when the block exits.
        """
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any): ...

class TarfileRd:
    """
    The main tar reader object. Enumerate over it with `async for`.

    Do not construct this class manually, instead use `open_rd` on the module.
    """
    async def close(self):
        """
        Close the archive.
        
        This operation renders the object useless for future operations.
        """

    def __aiter__(self) -> TarfileRd:
        """
        Enumerate members of the archive.
        
        When an archive is open for reading, you may use an `async for` block to iterate over the
        `TarfileEntry` objects comprising this archive. These objects MUST be used in order, and
        not used again after the next object is retrieved.
        """
    async def __anext__(self) -> TarfileEntry: ...
    async def __aenter__(self) -> TarfileRd:
        """
        Open the archive in a context manager.
        
        `TarfileRd` may be used in an `async with` block. This will cause `close()` to be
        automatically called when the block exits.
        """
    async def __aexit__(self, exc_type: Any, exc: Any, tb: Any): ...

class TarfileEntry:
    """
    A single member of a tar archive.
    """
    async def read(self, n: int = -1, /) -> bytes:
        """
        Read the contents of the entry.

        This method makes this object usable as an async bytestream.
        This method won't return anything useful on anything other than a regular file entry.
        """
    def name(self) -> bytes:
        """
        Retrieve the filepath of the entry as a bytestring.
        """
    def entry_type(self) -> TarfileEntryType:
        """
        Retrieve the type of the entry as a `TarfileEntryType` enum.
        """
    def mode(self) -> int:
        """
        Retrieve the mode, or permissions, of an entry as an int.
        """
    def size(self) -> int:
        """
        Retrieve the filesize of an entry as an int.
        """
    def link_target(self) -> bytes:
        """
        Retrieve the link target path of an entry as a bytestring.

        This method will raise an exception if used on an entry which is not a link.
        """

class TarfileEntryType:
    """
    An enum for types of tar entries.
    """
    Regular: TarfileEntryType
    Link: TarfileEntryType
    Symlink: TarfileEntryType
    Char: TarfileEntryType
    Block: TarfileEntryType
    Directory: TarfileEntryType
    Fifo: TarfileEntryType
    Continuous: TarfileEntryType
    GNULongName: TarfileEntryType
    GNULongLink: TarfileEntryType
    GNUSparse: TarfileEntryType
    XGlobalHeader: TarfileEntryType
    XHeader: TarfileEntryType
    Other: TarfileEntryType

class CompressionType:
    """
    An enum for supported types of tar compression.
    """
    Clear: CompressionType
    Gzip: CompressionType
    Bzip2: CompressionType
    Xz: CompressionType
    Detect: CompressionType
