use std::{borrow::BorrowMut, pin::Pin, sync::Arc, task::Poll};

use async_std::{
    io::{Read},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self};
use pyo3::{
    exceptions::{PyStopAsyncIteration},
    prelude::*,
    pyclass::IterANextOutput,
};

use crate::pyreader::PyReader;

#[pyclass]
/// The main tar object used for reading archives.
///
/// Do not construct this class manually, instead use `open_rd` on the module.
struct TarfileRd {
    archive: Arc<Mutex<AnyRdArchive>>,
}

enum AnyRdArchive {
    Clear(RdArchive<PyReader>),
    Gzip(RdArchive<async_compression::futures::write::GzipDecoder<PyReader>>),
    Bzip2(RdArchive<async_compression::futures::write::BzDecoder<PyReader>>),
    Xz(RdArchive<async_compression::futures::write::XzDecoder<PyReader>>),
}

impl Read for AnyRdArchive {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> Poll<std::io::Result<usize>> {
        match *self {
            AnyRdArchive::Clear(x) => x.poll_read(cx, buf),
            AnyRdArchive::Gzip(x) => x.poll_read(cx, buf),
            AnyRdArchive::Bzip2(x) => x.poll_read(cx, buf),
            AnyRdArchive::Xz(x) => x.poll_read(cx, buf),
        }
    }
}

enum RdArchive<R: Read + Unpin> {
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

            guard.become_stream();
            guard.check_error()?;

            let RdWrArchive::RdStream(entries) = (*guard).borrow_mut() else {
                return Err(AioTarfileError::new_err(
                    "Cannot iterate archive: archive not open for reading",
                ));
            };

            match entries.next().await {
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
                RdWrArchive::Rd(_) => {}
                RdWrArchive::RdStream(_) => {}
                RdWrArchive::Wr(wr) => {
                    wr.finish()
                        .await
                        .map_err(|_| AioTarfileError::new_err("Cannot close archive - unknown"))?;
                }
                RdWrArchive::Error(_) => {
                    unreachable!()
                }
            }

            *guard = RdWrArchive::Error(std::io::Error::new(
                std::io::ErrorKind::Other,
                "Archive is closed",
            ));
            Ok(Python::with_gil(|py| py.None()))
        })
    }
}
