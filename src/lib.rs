use std::{borrow::BorrowMut, future::Future, pin::Pin, sync::Arc, task::Poll};

use async_std::{
    io::{empty, Cursor, Read, Write},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self, EntryType, Header};
use pyo3::{
    create_exception,
    exceptions::{PyException, PyStopAsyncIteration, PyValueError},
    prelude::*,
    pyclass::IterANextOutput,
};

create_exception!(aiotarfile, AioTarfileError, PyException);

#[pyclass]
/// The main tar object.
///
/// Do not construct this class manually, instead use `open_rd` and `open_wr` on the module.
struct Tarfile {
    archive: Arc<Mutex<RdWrArchive>>,
}

struct PyReader {
    fp: Py<PyAny>,
    fut: Option<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Sync + Send>>>,
}

impl PyReader {
    fn new<R: Into<Py<PyAny>>>(fp: R) -> Self {
        Self {
            fp: fp.into(),
            fut: None,
        }
    }
}

impl Read for PyReader {
    fn poll_read(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut [u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        if let Some(fut) = &mut self.fut {
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(s)) => {
                    self.fut = None;
                    return Python::with_gil(move |py| {
                        match s.downcast::<pyo3::types::PyBytes>(py) {
                            Ok(s) => {
                                let s = s.as_bytes();
                                buf[..s.len()].copy_from_slice(s);
                                Poll::Ready(Ok(s.len()))
                            }
                            Err(_) => Poll::Ready(Err(std::io::Error::new(
                                std::io::ErrorKind::Other,
                                "await read() did not return bytes",
                            ))),
                        }
                    });
                }
                Poll::Ready(Err(e)) => {
                    self.fut = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        } else {
            let fp = self.fp.clone();
            let len = buf.len();
            match Python::with_gil(move |py| match fp.call_method1(py, "read", (len,)) {
                Ok(coro) => match pyo3_asyncio::async_std::into_future(coro.as_ref(py)) {
                    Ok(fut) => {
                        self.fut = Some(Box::pin(fut));
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }) {
                Ok(()) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Err(e) => Poll::Ready(Err(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    format!("could not create read() coroutine(): {}", e),
                ))),
            }
        }
    }
}

struct PyWriter {
    fp: Py<PyAny>,
    fut: Option<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Sync + Send>>>,
}

impl PyWriter {
    fn new<R: Into<Py<PyAny>>>(fp: R) -> Self {
        Self {
            fp: fp.into(),
            fut: None,
        }
    }
}

impl Write for PyWriter {
    fn poll_write(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        if let Some(fut) = &mut self.fut {
            match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(r)) => {
                    self.fut = None;
                    return Python::with_gil(move |py| match r.extract::<usize>(py) {
                        Ok(s) => Poll::Ready(Ok(s)),
                        Err(_) => Poll::Ready(Err(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            format!(
                                "await write() did not return int or int is out of range: {}",
                                r.to_string()
                            ),
                        ))),
                    });
                }
                Poll::Ready(Err(e)) => {
                    self.fut = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }

        let fp = self.fp.clone();
        match Python::with_gil(move |py| {
            let buf = pyo3::types::PyBytes::new(py, buf);
            match fp.call_method1(py, "write", (buf,)) {
                Ok(coro) => match pyo3_asyncio::async_std::into_future(coro.as_ref(py)) {
                    Ok(fut) => {
                        self.fut = Some(Box::pin(fut));
                        Ok(())
                    }
                    Err(e) => Err(e),
                },
                Err(e) => Err(e),
            }
        }) {
            Ok(()) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("could not create write() coroutine(): {}", e),
            ))),
        }
    }

    fn poll_flush(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(fut) = &mut self.fut {
            return match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    self.fut = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    self.fut = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            };
        }

        let fp = self.fp.clone();
        match Python::with_gil(move |py| match fp.call_method0(py, "flush") {
            Ok(coro) => match pyo3_asyncio::async_std::into_future(coro.as_ref(py)) {
                Ok(fut) => {
                    self.fut = Some(Box::pin(fut));
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }) {
            Ok(()) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("could not create flush() coroutine(): {}", e),
            ))),
        }
    }

    fn poll_close(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        if let Some(fut) = &mut self.fut {
            return match Pin::new(fut).poll(cx) {
                Poll::Ready(Ok(_)) => {
                    self.fut = None;
                    Poll::Ready(Ok(()))
                }
                Poll::Ready(Err(e)) => {
                    self.fut = None;
                    return Poll::Ready(Err(std::io::Error::new(std::io::ErrorKind::Other, e)));
                }
                Poll::Pending => {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            };
        }

        let fp = self.fp.clone();
        match Python::with_gil(move |py| match fp.call_method0(py, "close") {
            Ok(coro) => match pyo3_asyncio::async_std::into_future(coro.as_ref(py)) {
                Ok(fut) => {
                    self.fut = Some(Box::pin(fut));
                    Ok(())
                }
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        }) {
            Ok(()) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Err(e) => Poll::Ready(Err(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("could not create close() coroutine(): {}", e),
            ))),
        }
    }
}

enum RdWrArchive {
    Error(std::io::Error),
    Rd(async_tar::Archive<PyReader>),
    RdStream(async_tar::Entries<PyReader>),
    Wr(async_tar::Builder<PyWriter>),
}

impl RdWrArchive {
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
                RdWrArchive::Rd(r) => match r.entries() {
                    Ok(s) => Self::RdStream(s),
                    Err(e) => Self::Error(e),
                },
                _ => iself,
            },
        );
    }

    fn check_error(&self) -> PyResult<()> {
        if let RdWrArchive::Error(e) = self {
            Err(PyValueError::new_err(format!(
                "Archive entered errored state: {}",
                e
            )))
        } else {
            Ok(())
        }
    }
}

#[pyfunction]
/// Open a tar file for reading.
///
/// This function takes an asynchronous stream, i.e. an object with `async def read(self, n=-1) -> bytes`
/// It returns a `Tarfile` object.
fn open_rd(fp: &PyAny) -> PyResult<Tarfile> {
    Ok(Tarfile {
        archive: Arc::new(Mutex::new(RdWrArchive::Rd(async_tar::Archive::new(
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
fn open_wr(fp: &PyAny) -> PyResult<Tarfile> {
    Ok(Tarfile {
        archive: Arc::new(Mutex::new(RdWrArchive::Wr(async_tar::Builder::new(
            PyWriter::new(fp),
        )))),
    })
}

#[pymethods]
impl Tarfile {
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
            let RdWrArchive::Wr(archive) = (*guard).borrow_mut() else {
                return Err(PyValueError::new_err(
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
                    .map_err(|e| PyValueError::new_err(format!("Cannot add file: {}", e)))?;
            } else {
                let mut buffered = vec![];
                PyReader::new(content)
                    .read_to_end(&mut buffered)
                    .await
                    .map_err(|e| PyValueError::new_err(e))?;
                header.set_size(buffered.len() as u64);
                archive
                    .append_data(&mut header, name, Cursor::new(buffered))
                    .await
                    .map_err(|e| PyValueError::new_err(format!("Cannot add file: {}", e)))?;
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
            let RdWrArchive::Wr(archive) = (*guard).borrow_mut() else {
                return Err(PyValueError::new_err(
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
                .map_err(|e| PyValueError::new_err(e))?;
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
            let RdWrArchive::Wr(archive) = (*guard).borrow_mut() else {
                return Err(PyValueError::new_err(
                    "Cannot add file: archive not open for writing",
                ));
            };
            let mut header = Header::new_gnu();
            header.set_mode(mode);
            header.set_size(0);
            header.set_entry_type(EntryType::symlink());
            header
                .set_link_name(target)
                .map_err(|e| PyValueError::new_err(e))?;
            archive
                .append_data(&mut header, name, empty())
                .await
                .map_err(|e| PyValueError::new_err(e))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    /// Close the archive.
    ///
    /// This operation finalizes and flushes the output stream if writing and then renders the
    /// object useless for future operations.
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
                        .map_err(|_| PyValueError::new_err("Cannot close archive - unknown"))?;
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
                return Err(PyValueError::new_err(
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
                Some(Err(e)) => Err(PyValueError::new_err(e.to_string())),
                None => Err(PyStopAsyncIteration::new_err(())),
            }
        })
        .map(|a| IterANextOutput::Yield(a))
    }

    /// Open the archive in a context manager.
    ///
    /// `Tarfile` may be used in an `async with` block. This will cause `close()` to be
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
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        Ok(pyo3::types::PyBytes::new(py, guard.path_bytes().as_ref()))
    }

    /// Retrieve the type of the entry as a `TarfileEntryType` enum.
    fn entry_type(&self) -> PyResult<TarfileEntryType> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        Ok(guard.header().entry_type().into())
    }

    /// Retrieve the mode, or permissions, of an entry as an int.
    fn mode(&self) -> PyResult<u32> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        guard.header().mode().map_err(|e| PyValueError::new_err(e))
    }

    /// Retrieve the link target path of an entry as a bytestring.
    ///
    /// This method will raise an exception if used on an entry which is not a link.
    fn link_target<'p>(&self, py: Python<'p>) -> PyResult<&'p pyo3::types::PyBytes> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        let Some(bytes) = guard.link_name_bytes() else {
            return Err(PyValueError::new_err("Not a link"));
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
                    .map_err(|e| PyValueError::new_err(e))?;
                Ok(Python::with_gil(|py| {
                    pyo3::types::PyBytes::new(py, &buf).to_object(py)
                }))
            } else {
                let mut buf = vec![0; n as usize];
                let real_n = entry
                    .read(&mut buf)
                    .await
                    .map_err(|e| PyValueError::new_err(e))?;
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
