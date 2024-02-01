use std::{future::Future, pin::Pin, task::Poll};

use async_std::io::BufReader;
use pyo3::prelude::*;

use crate::AioTarfileError;

pub struct PyReader {
    fp: Py<PyAny>,
    fut: Option<Pin<Box<dyn Future<Output = PyResult<PyObject>> + Sync + Send>>>,
}

impl PyReader {
    pub fn new<R: Into<Py<PyAny>>>(fp: R) -> Self {
        Self {
            fp: fp.into(),
            fut: None,
        }
    }

    pub fn new_buffered<R: Into<Py<PyAny>>>(fp: R) -> BufReader<Self> {
        BufReader::new(Self::new(fp))
    }
}

impl async_std::io::Read for PyReader {
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
