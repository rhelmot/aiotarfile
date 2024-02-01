use std::sync::Arc;

use async_std::{io::Read, prelude::*, sync::Mutex};
use async_tar::{self, EntryType};
use peek_buf_reader::BufReader;
use pyo3::{create_exception, exceptions::PyException, prelude::*};
use pyreader::PyReader;
use pywriter::PyWriter;
use rd::{RdArchive, TarfileRd};
use wr::TarfileWr;

#[pyclass]
#[derive(Copy, Clone)]
/// An enum for supported types of tar compression.
enum CompressionType {
    Clear,
    Gzip,
    Bzip2,
    Xz,
    Detect,
}

create_exception!(aiotarfile, AioTarfileError, PyException);

mod peek_buf_reader;
mod pyreader;
mod pywriter;
mod rd;
mod wr;

#[pyfunction]
#[pyo3(signature = (fp, compression = CompressionType::Detect))]
/// Open a tar file for reading.
///
/// This function takes an asynchronous stream, i.e. an object with `async def read(self, n=-1) -> bytes`
/// It returns a `TarfileRd` object.
fn open_rd<'p>(py: Python<'p>, fp: &'p PyAny, compression: CompressionType) -> PyResult<&'p PyAny> {
    let fp = PyReader::new_buffered(fp);
    pyo3_asyncio::async_std::future_into_py(py, async move {
        Ok(TarfileRd {
            archive: Arc::new(Mutex::new(RdArchive::Rd(async_tar::Archive::new(
                match compression {
                    CompressionType::Clear => Box::new(fp),
                    CompressionType::Gzip => {
                        Box::new(async_compression::futures::bufread::GzipDecoder::new(fp))
                    }
                    CompressionType::Bzip2 => {
                        Box::new(async_compression::futures::bufread::BzDecoder::new(fp))
                    }
                    CompressionType::Xz => {
                        Box::new(async_compression::futures::bufread::XzDecoder::new(fp))
                    }
                    CompressionType::Detect => {
                        // moderate nightmares commence
                        let mut fp = fp.into_inner();
                        let mut peek = [0u8; 263];
                        let mut bufsize = 0;
                        while bufsize < 263 {
                            let result = fp
                                .read(&mut peek[bufsize..])
                                .await
                                .map_err(|e| AioTarfileError::new_err(e))?;
                            if result == 0 {
                                break;
                            }
                            bufsize += result;
                        }
                        let compression = match peek {
                            [.., 0x75, 0x73, 0x74, 0x61, 0x72, _] => CompressionType::Clear,
                            [0x1f, 0x8b, ..] => CompressionType::Gzip,
                            [0x42, 0x5a, 0x68, ..] => CompressionType::Bzip2,
                            [0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, ..] => CompressionType::Xz,
                            _ => {
                                return Err(AioTarfileError::new_err(
                                    "Unsupported compression or bad data",
                                ));
                            }
                        };

                        let fp = BufReader::with_inital_buffer(&peek, 4096, fp);
                        match compression {
                            CompressionType::Clear => Box::new(fp),
                            CompressionType::Gzip => {
                                Box::new(async_compression::futures::bufread::GzipDecoder::new(fp))
                            }
                            CompressionType::Bzip2 => {
                                Box::new(async_compression::futures::bufread::BzDecoder::new(fp))
                            }
                            CompressionType::Xz => {
                                Box::new(async_compression::futures::bufread::XzDecoder::new(fp))
                            }
                            CompressionType::Detect => unreachable!(),
                        }
                    }
                },
            )))),
        })
    })
}

#[pyfunction]
#[pyo3(signature = (fp, compression = CompressionType::Clear))]
/// Open a tar file for writing.
///
/// This function takes an asynchronous stream, i.e. an object with `async def write(self, buf: bytes) -> int`
/// and `async def close(self)`
/// It returns a `TarfileWr` object.
fn open_wr<'p>(py: Python<'p>, fp: &'p PyAny, compression: CompressionType) -> PyResult<&'p PyAny> {
    let fp = PyWriter::new(fp);
    pyo3_asyncio::async_std::future_into_py(py, async move {
        Ok(TarfileWr {
            archive: Arc::new(Mutex::new(Ok(async_tar::Builder::new(match compression {
                CompressionType::Clear => Box::new(fp),
                CompressionType::Gzip => {
                    Box::new(async_compression::futures::write::GzipEncoder::new(fp))
                }
                CompressionType::Bzip2 => {
                    Box::new(async_compression::futures::write::BzEncoder::new(fp))
                }
                CompressionType::Xz => {
                    Box::new(async_compression::futures::write::XzEncoder::new(fp))
                }
                CompressionType::Detect => {
                    return Err(AioTarfileError::new_err(
                        "Cannot detect compression for writing new archive",
                    ));
                }
            })))),
        })
    })
}

#[pyclass]
/// A single member of a tar archive.
struct TarfileEntry {
    entry: Arc<Mutex<async_tar::Entry<async_tar::Archive<Box<dyn Read + Unpin + Send>>>>>,
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
        guard
            .header()
            .mode()
            .map_err(|e| AioTarfileError::new_err(e))
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
    m.add_class::<TarfileRd>()?;
    m.add_class::<TarfileWr>()?;
    m.add_class::<TarfileEntry>()?;
    m.add_class::<TarfileEntryType>()?;
    m.add_class::<CompressionType>()?;
    m.add("AioTarfileError", py.get_type::<AioTarfileError>())?;
    Ok(())
}
