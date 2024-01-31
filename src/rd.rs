use std::{borrow::BorrowMut, pin::Pin, sync::Arc, task::Poll};

use async_std::{
    io::{Read, BufReader},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self};
use pyo3::{
    exceptions::PyStopAsyncIteration,
    prelude::*,
    pyclass::IterANextOutput,
};

use crate::pyreader::PyReader;
use crate::{AioTarfileError, TarfileEntry};

#[pyclass]
/// The main tar object used for reading archives.
///
/// Do not construct this class manually, instead use `open_rd` on the module.
pub struct TarfileRd {
    pub archive: Arc<Mutex<AnyRdArchive>>,
}

enum AnyRdArchive {
    Clear(RdArchive<PyReader>),
    Gzip(RdArchive<async_compression::futures::bufread::GzipDecoder<BufReader<PyReader>>>),
    Bzip2(RdArchive<async_compression::futures::bufread::BzDecoder<BufReader<PyReader>>>),
    Xz(RdArchive<async_compression::futures::bufread::XzDecoder<BufReader<PyReader>>>),
}

impl AnyRdArchive {
    fn check_error(&self) -> PyResult<()> {
        match self {
            AnyRdArchive::Clear(x) => x.check_error(),
            AnyRdArchive::Gzip(x) => x.check_error(),
            AnyRdArchive::Bzip2(x) => x.check_error(),
            AnyRdArchive::Xz(x) => x.check_error(),
        }
    }
}

pub enum RdArchive<R: Read + Unpin> {
    Error(std::io::Error),
    Rd(async_tar::Archive<R>),
    RdStream(async_tar::Entries<R>),
}

impl<R: Read + Unpin> RdArchive<R> {
    fn become_stream(&mut self) {
        replace_with::replace_with(
            self,
            || {
                Self::Error(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "Panic when reading header",
                ))
            },
            |iself| match iself {
                RdArchive::Rd(r) => match r.entries() {
                    Ok(s) => Self::RdStream(s),
                    Err(e) => Self::Error(e),
                },
                _ => iself,
            },
        );
    }

    fn check_error(&self) -> PyResult<()> {
        if let RdArchive::Error(e) = self {
            Err(AioTarfileError::new_err(format!(
                "Archive entered errored state: {}",
                e
            )))
        } else {
            Ok(())
        }
    }

    async fn next(&mut self) -> Result<Option<async_tar::Entry<async_tar::Archive<PyReader>>>, std::io::Error> {
        match self {
            RdArchive::Error(e) => Ok(Err(e)),
            RdArchive::Rd(_) => {
                self.become_stream();
                self.next().await
            },
            RdArchive::RdStream(s) => s.next().await,
        }
    }
}

#[pymethods]
impl TarfileRd {
    /// Enumerate members of the archive.
    ///
    /// When an archive is open for reading, you may use an `async for` block to iterate over the
    /// `TarfileEntry` objects comprising this archive. These objects MUST be used in order, and
    /// not used again after the next object is retrieved.
    fn __aiter__(this: Py<Self>) -> PyResult<Py<Self>> {
        Ok(this)
    }

    // https://github.com/PyO3/pyo3/issues/3190 is set to stabilize in 0.22
    // This should de-jankify this function
    fn __anext__<'p>(&self, py: Python<'p>) -> PyResult<IterANextOutput<&'p PyAny, &'p PyAny>> {
        let archive = self.archive.clone();
        pyo3_asyncio::async_std::future_into_py::<_, PyObject>(py, async move {
            let mut guard = archive.lock().await;

            match guard.next().await {
                Some(Ok(entry)) => Python::with_gil(|py| {
                    Ok(TarfileEntry {
                        entry: Arc::new(Mutex::new(entry)),
                    }
                    .into_py(py))
                }),
                Some(Err(e)) => Err(AioTarfileError::new_err(e.to_string())),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
        .map(|a| IterANextOutput::Yield(a))
    }

    /// Open the archive in a context manager.
    ///
    /// `TarfileRd` may be used in an `async with` block. This will cause `close()` to be
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

    /// Close the archive.
    ///
    /// This operation renders the object useless for future operations.
    fn close<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let archive = self.archive.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut guard = archive.lock().await;
            guard.check_error()?;
            match (*guard).borrow_mut() {
                RdArchive::Rd(_) => {}
                RdArchive::RdStream(_) => {}
                RdArchive::Wr(wr) => {
                    wr.finish()
                        .await
                        .map_err(|_| AioTarfileError::new_err("Cannot close archive - unknown"))?;
                }
                RdArchive::Error(_) => {
                    unreachable!()
                }
            }

            *guard = RdArchive::Error(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Archive is closed",
            ));
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}
