use std::{borrow::BorrowMut, future::Future, pin::Pin, sync::Arc, task::Poll};

use async_std::{
    io::{Read, Write, Cursor, empty},
    prelude::*,
    sync::Mutex,
};
use async_tar::{self, EntryType, Header};
use pyo3::{
    exceptions::{PyStopAsyncIteration, PyValueError},
    prelude::*,
    pyclass::IterANextOutput,
};

#[pyclass]
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
fn open_rd(fp: &PyAny) -> PyResult<Tarfile> {
    Ok(Tarfile {
        archive: Arc::new(Mutex::new(RdWrArchive::Rd(async_tar::Archive::new(
            PyReader::new(fp),
        )))),
    })
}

#[pyfunction]
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
                PyReader::new(content).read_to_end(&mut buffered).await
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

    fn add_dir<'p>(
        &self,
        py: Python<'p>,
        name: &'p str,
        mode: u32,
    ) -> PyResult<&'p PyAny> {
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
            archive.append_data(&mut header, name, empty()).await.map_err(|e| PyValueError::new_err(e))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

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
            header.set_link_name(target).map_err(|e| PyValueError::new_err(e))?;
            archive.append_data(&mut header, name, empty()).await.map_err(|e| PyValueError::new_err(e))?;
            Ok(Python::with_gil(|py| py.None()))
        })
    }

    fn close<'p>(&self, py: Python<'p>) -> PyResult<&'p PyAny> {
        let archive = self.archive.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut guard = archive.lock().await;
            guard.check_error()?;
            match (*guard).borrow_mut() {
                RdWrArchive::Rd(_) => todo!(),
                RdWrArchive::RdStream(_) => todo!(),
                RdWrArchive::Wr(wr) => {
                    wr.finish()
                        .await
                        .map_err(|_| PyValueError::new_err("Cannot close archive - unknown"))?;
                }
                RdWrArchive::Error(_) => {
                    unreachable!()
                }
            }

            Ok(Python::with_gil(|py| py.None()))
        })
    }

    fn __aiter__(this: Py<Self>) -> PyResult<Py<Self>> {
        Ok(this)
    }

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
}

#[pyclass]
struct TarfileEntry {
    entry: Arc<Mutex<async_tar::Entry<async_tar::Archive<PyReader>>>>,
}

#[pymethods]
impl TarfileEntry {
    fn name<'p>(&self, py: Python<'p>) -> PyResult<&'p pyo3::types::PyBytes> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        Ok(pyo3::types::PyBytes::new(py, guard.path_bytes().as_ref()))
    }

    fn entry_type(&self) -> PyResult<TarfileEntryType> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        Ok(guard.header().entry_type().into())
    }

    fn mode(&self) -> PyResult<u32> {
        let Some(guard) = self.entry.try_lock() else {
            return Err(PyValueError::new_err("Another operation is in progress"));
        };
        guard.header().mode().map_err(|e| PyValueError::new_err(e))
    }

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
    fn read<'p>(&self, py: Python<'p>, n: isize) -> PyResult<&'p PyAny> {
        let entry = self.entry.clone();
        pyo3_asyncio::async_std::future_into_py(py, async move {
            let mut entry = entry.lock().await;
            if n < 0 {
                let mut buf = vec![];
                entry.read_to_end(&mut buf).await.map_err(|e| PyValueError::new_err(e))?;
                Ok(Python::with_gil(|py| pyo3::types::PyBytes::new(py, &buf).to_object(py)))
            } else {
                let mut buf = vec![0; n as usize];
                let real_n = entry.read(&mut buf).await.map_err(|e| PyValueError::new_err(e))?;
                Ok(Python::with_gil(|py| pyo3::types::PyBytes::new(py, &buf[..real_n]).to_object(py)))
            }
        })
    }
}

#[pyclass]
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
fn aiotarfile(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(open_rd, m)?)?;
    m.add_function(wrap_pyfunction!(open_wr, m)?)?;
    m.add_class::<Tarfile>()?;
    m.add_class::<TarfileEntry>()?;
    m.add_class::<TarfileEntryType>()?;
    Ok(())
}
