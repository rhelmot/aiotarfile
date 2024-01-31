use std::{borrow::BorrowMut, future::Future, pin::Pin, sync::Arc, task::Poll};

use async_std::{
    io::{empty, Cursor, Read, Write},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self, EntryType, Header};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyStopAsyncIteration},
    prelude::*,
    pyclass::IterANextOutput,
};
use pyreader::PyReader;
use rd::{TarfileRd, RdArchive};
use wr::{TarfileRd, RdArchive};

#[pyclass]
enum CompressionType {
    Clear,
    Gzip,
    Bzip2,
    Xz,
    Detect,
}

create_exception!(aiotarfile, AioTarfileError, PyException);

mod rd;
mod wr;
mod pyreader;
mod pywriter;

#[pyfunction]
/// Open a tar file for reading.
///
/// This function takes an asynchronous stream, i.e. an object with `async def read(self, n=-1) -> bytes`
/// It returns a `Tarfile` object.
fn open_rd(fp: &PyAny, compression: &CompressionType) -> PyResult<TarfileRd> {
    Ok(TarfileRd {
        archive: Arc::new(Mutex::new(RdArchive::Rd(async_tar::Archive::new(
            PyReader::new(fp),
        )))),
    })
}

#[pyfunction]
/// Open a tar file for writing.
///
/// This function takes an asynchronous stream, i.e. an object with `async def write(self, buf: bytes) -> int`
/// and `async def close(self)`
/// It returns a `Tarfile` object.
fn open_wr(fp: &PyAny) -> PyResult<TarfileRd> {
    Ok(TarfileRd {
        archive: Arc::new(Mutex::new(WrArchive::Wr(async_tar::Builder::new(
            PyWriter::new(fp),
        )))),
    })
}

#[pyclass]
/// A single member of a tar archive.
struct TarfileEntry {
    entry: Arc<Mutex<async_tar::Entry<async_tar::Archive<PyReader>>>>,
}

#[pymethods]
impl TarfileEntry {
    /// Retrieve the filepath of the entry as a bytestring.
    fn name<'p>(&self, py: Python<'p>) -> PyResult<&'p pyo3::types::PyBytes> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(AioTarfileError::new_err("Another operation is in progress"));
        };
        Ok(pyo3::types::PyBytes::new(py, guard.path_bytes().as_ref()))
    }

    /// Retrieve the type of the entry as a `TarfileEntryType` enum.
    fn entry_type(&self) -> PyResult<TarfileEntryType> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(AioTarfileError::new_err("Another operation is in progress"));
        };
        Ok(guard.header().entry_type().into())
    }

    /// Retrieve the mode, or permissions, of an entry as an int.
    fn mode(&self) -> PyResult<u32> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(AioTarfileError::new_err("Another operation is in progress"));
        };
        guard.header().mode().map_err(|e| AioTarfileError::new_err(e))
    }

    /// Retrieve the link target path of an entry as a bytestring.
    ///
    /// This method will raise an exception if used on an entry which is not a link.
    fn link_target<'p>(&self, py: Python<'p>) -> PyResult<&'p pyo3::types::PyBytes> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(AioTarfileError::new_err("Another operation is in progress"));
        };
        let Some(bytes) = guard.link_name_bytes() else {
            return Err(AioTarfileError::new_err("Not a link"));
        };
        Ok(pyo3::types::PyBytes::new(py, bytes.as_ref()))
    }

    #[pyo3(signature = (n = -1))]
    /// Read the contents of the entry.
    ///
    /// This method makes this object usable as an async bytestream.
    /// This method won't return anything useful on anything other than a regular file entry.
    fn read<'p>(&self, py: Python<'p>, n: isize) -> PyResult<&'p PyAny> {
        let entry = self.entry.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut entry = entry.lock().await;
            if n < 0 {
                let mut buf = vec![];
                entry
                    .read_to_end(&mut buf)
                    .await
                    .map_err(|e| AioTarfileError::new_err(e))?;
                Ok(Python::with_gil(|py| {
                    pyo3::types::PyBytes::new(py, &buf).to_object(py)
                }))
            } else {
                let mut buf = vec![0; n as usize];
                let real_n = entry
                    .read(&mut buf)
                    .await
                    .map_err(|e| AioTarfileError::new_err(e))?;
                Ok(Python::with_gil(|py| {
                    pyo3::types::PyBytes::new(py, &buf[..real_n]).to_object(py)
                }))
            }
        })
    }
}

#[pyclass]
/// An enum for types of tar entries.
enum TarfileEntryType {
    Regular,
    Link,
    Symlink,
    Char,
    Block,
    Directory,
    Fifo,
    Continuous,
    GNULongName,
    GNULongLink,
    GNUSparse,
    XGlobalHeader,
    XHeader,
    Other,
}

impl From<async_tar::EntryType> for TarfileEntryType {
    fn from(value: async_tar::EntryType) -> Self {
        match value {
            EntryType::Regular => Self::Regular,
            EntryType::Link => Self::Link,
            EntryType::Symlink => Self::Symlink,
            EntryType::Char => Self::Char,
            EntryType::Block => Self::Block,
            EntryType::Directory => Self::Directory,
            EntryType::Fifo => Self::Fifo,
            EntryType::Continuous => Self::Continuous,
            EntryType::GNULongName => Self::GNULongName,
            EntryType::GNULongLink => Self::GNULongLink,
            EntryType::GNUSparse => Self::GNUSparse,
            EntryType::XGlobalHeader => Self::XGlobalHeader,
            EntryType::XHeader => Self::XHeader,
            _ => Self::Other,
        }
    }
}

#[pymodule]
/// A module for asynchronous access to reading and writing streaming tarballs.
///
/// Your entry points should be `open_rd` and `open_wr`.
fn aiotarfile(py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_rd, m)?)?;
    m.add_function(wrap_pyfunction!(open_wr, m)?)?;
    m.add_class::<Tarfile>()?;
    m.add_class::<TarfileEntry>()?;
    m.add_class::<TarfileEntryType>()?;
    m.add("AioTarfileError", py.get_type::<AioTarfileError>())?;
    Ok(())
}
