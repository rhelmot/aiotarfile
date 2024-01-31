use std::{borrow::BorrowMut, sync::Arc, pin::Pin};

use async_std::{
    io::{empty, Cursor, Write},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self, EntryType, Header};
use pyo3::prelude::*;

use crate::pywriter::PyWriter;
use crate::pyreader::PyReader;
use crate::AioTarfileError;

#[pyclass]
/// The main tar object used for writing archives.
///
/// Do not construct this class manually, instead use `open_wr` on the module.
struct TarfileWr {
    archive: Arc<Mutex<Result<AnyWrArchive, std::io::Error>>>,
}

enum AnyWrArchive {
    Clear(WrArchive<PyWriter>),
    Gzip(WrArchive<async_compression::futures::write::GzipEncoder<PyWriter>>),
    Bzip2(WrArchive<async_compression::futures::write::BzEncoder<PyWriter>>),
    Xz(WrArchive<async_compression::futures::write::XzEncoder<PyWriter>>),
}

impl Write for AnyWrArchive {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.get_mut() {
            AnyWrArchive::Clear(x) => Pin::new(x).poll_write(cx, buf),
            AnyWrArchive::Gzip(x) => x.poll_write(cx, buf),
            AnyWrArchive::Bzip2(x) => x.poll_write(cx, buf),
            AnyWrArchive::Xz(x) => x.poll_write(cx, buf),
        }
    }

    fn poll_flush(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            AnyWrArchive::Clear(x) => x.poll_flush(cx),
            AnyWrArchive::Gzip(x) => x.poll_flush(cx),
            AnyWrArchive::Bzip2(x) => x.poll_flush(cx),
            AnyWrArchive::Xz(x) => x.poll_flush(cx),
        }
    }

    fn poll_close(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<()>> {
        match *self {
            AnyWrArchive::Clear(x) => x.poll_close(cx),
            AnyWrArchive::Gzip(x) => x.poll_close(cx),
            AnyWrArchive::Bzip2(x) => x.poll_close(cx),
            AnyWrArchive::Xz(x) => x.poll_close(cx),
        }
    }
}

enum WrArchive<W: Write + Unpin + Send + Sync> {
    Error(std::io::Error),
    Wr(async_tar::Builder<W>),
}

#[pymethods]
impl TarfileWr {
    #[pyo3(signature = (name, mode, content, size = None))]
    /// Add a regular file to the archive.
    ///
    /// `content` should be an asynchronous stream, i.e. and object with
    /// `async def read(self, n=-1) -> bytes`.
    ///
    /// Warning: if `size` is not provided, the entire contents of the input stream will be
    /// buffered into memory in order to count its size.
    fn add_file<'p>(
        &self,
        py: Python<'p>,
        name: &'p str,
        mode: u32,
        content: &'p PyAny,
        size: Option<u64>,
    ) -> PyResult<&'p PyAny> {
        let content: Py<PyAny> = content.into();
        let archive = self.archive.clone();
        let name = name.to_string();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut guard = archive.lock().await;
            let Ok(archive) = (*guard).borrow_mut() else {
                return Err(AioTarfileError::new_err(
                    "Cannot add file: archive not open for writing",
                ));
            };
            let mut header = Header::new_gnu();
            header.set_mode(mode);
            header.set_entry_type(EntryType::file());
            if let Some(size) = size {
                header.set_size(size);
                archive
                    .append_data(&mut header, name, PyReader::new(content))
                    .await
                    .map_err(|e| AioTarfileError::new_err(format!("Cannot add file: {}", e)))?;
            } else {
                let mut buffered = vec![];
                PyReader::new(content)
                    .read_to_end(&mut buffered)
                    .await
                    .map_err(|e| AioTarfileError::new_err(e))?;
                header.set_size(buffered.len() as u64);
                archive
                    .append_data(&mut header, name, Cursor::new(buffered))
                    .await
                    .map_err(|e| AioTarfileError::new_err(format!("Cannot add file: {}", e)))?;
            }
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Add a directory to the archive.
    fn add_dir<'p>(&self, py: Python<'p>, name: &'p str, mode: u32) -> PyResult<&'p PyAny> {
        let archive = self.archive.clone();
        let name = name.to_string();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut guard = archive.lock().await;
            let Ok(archive) = (*guard).borrow_mut() else {
                return Err(AioTarfileError::new_err(
                    "Cannot add file: archive not open for writing",
                ));
            };
            let mut header = Header::new_gnu();
            header.set_mode(mode);
            header.set_size(0);
            header.set_entry_type(EntryType::dir());
            archive
                .append_data(&mut header, name, empty())
                .await
                .map_err(|e| AioTarfileError::new_err(e))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Add a symlink to the archive.
    fn add_symlink<'p>(
        &self,
        py: Python<'p>,
        name: &'p str,
        mode: u32,
        target: &'p str,
    ) -> PyResult<&'p PyAny> {
        let archive = self.archive.clone();
        let name = name.to_string();
        let target = target.to_string();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut guard = archive.lock().await;
            let Ok(archive) = (*guard).borrow_mut() else {
                return Err(AioTarfileError::new_err(
                    "Cannot add file: archive not open for writing",
                ));
            };
            let mut header = Header::new_gnu();
            header.set_mode(mode);
            header.set_size(0);
            header.set_entry_type(EntryType::symlink());
            header
                .set_link_name(target)
                .map_err(|e| AioTarfileError::new_err(e))?;
            archive
                .append_data(&mut header, name, empty())
                .await
                .map_err(|e| AioTarfileError::new_err(e))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Close the archive.
    ///
    /// This operation finalizes and flushes the output stream and then renders the
    /// object useless for future operations.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let archive = self.archive.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut guard = archive.lock().await;
            guard.check_error()?;
            match (*guard).borrow_mut() {
                Ok(wr) => {
                    wr.finish()
                        .await
                        .map_err(|_| AioTarfileError::new_err("Cannot close archive - unknown"))?;
                }
                Err(_) => {
                    unreachable!()
                }
            }

            *guard = Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Archive is closed",
            ));
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Open the archive in a context manager.
    ///
    /// `TarfileWr` may be used in an `async with` block. This will cause `close()` to be
    /// automatically called when the block exits.
    fn __aenter__<'p>(this: Py<Self>, py: Python<'p>) -> PyResult<&'p PyAny> {
        pyo3_asyncio::async_std::future_into_py(py, async move { Ok(this) })
    }

    fn __aexit__<'p>(
        &self,
        py: Python<'p>,
        _exc_type: &'p PyAny,
        _exc: &'p PyAny,
        _tb: &'p PyAny,
    ) -> PyResult<&'p PyAny> {
        self.close(py)
    }
}

