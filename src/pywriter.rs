use std::{future::Future, io, pin::Pin, task::Poll};

use async_std::io::Write;
use pyo3::prelude::*;

pub struct PyWriter {
    fp: Py<PyAny>,
    fut: Option<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Sync + Send>>>,
}

impl PyWriter {
    pub fn new<R: Into<Py<PyAny>>>(fp: R) -> Self {
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
        match Python::with_gil(move |py| -> Result<bool, io::Error> {
            let Some(flush) = fp.getattr(py, "flush").ok() else {
                return Ok(false);
            };

            self.fut = Some(Box::pin(pyo3_asyncio::async_std::into_future(
                flush.call0(py)?.as_ref(py),
            )?));
            Ok(true)
        }) {
            Ok(true) => {
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Ok(false) => Poll::Ready(Ok(())),
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
