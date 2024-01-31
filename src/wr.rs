use std::{borrow::BorrowMut, io, sync::Arc};

use async_std::{
    io::{empty, Cursor, Write},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self, EntryType, Header};
use futures_util::io::AsyncWriteExt;
use pyo3::prelude::*;

use crate::pyreader::PyReader;
use crate::AioTarfileError;

#[pyclass]
/// The main tar object used for writing archives.
///
/// Do not construct this class manually, instead use `open_wr` on the module.
pub struct TarfileWr {
    pub archive:
        Arc<Mutex<Result<async_tar::Builder<Box<dyn Write + Unpin + Send + Sync>>, io::Error>>>,
}

#[pymethods]
/// The main tar builder object.
///
/// Do not construct this class manually, instead use `open_wr` on the module.
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
            let archive = guard
                .as_mut()
                .map_err(|e| AioTarfileError::new_err(e.to_string()))?;
            let mut header = Header::new_gnu();
            header.set_mode(mode);
            header.set_entry_type(EntryType::file());
            if let Some(size) = size {
                header.set_size(size);
                archive
                    .append_data(&mut header, &name, &mut PyReader::new(content))
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
                    .append_data(&mut header, &name, &mut Cursor::new(buffered))
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
                .append_data(&mut header, &name, &mut empty())
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
                .append_data(&mut header, &name, &mut empty())
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
            //let archive = guard
            //    .as_mut()
            //    .map_err(|e| AioTarfileError::new_err(e.to_string()))?;
            let archive2 = &mut (*guard);
            let mut archive = Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Archive is closed",
            ));
            std::mem::swap(&mut archive, archive2);
            archive?
                .into_inner()
                .await
                .map_err(|e| AioTarfileError::new_err(e))?
                .close()
                .await?;

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
