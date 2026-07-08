//! Python bindings for emstar via PyO3.
//!
//! ```python
//! import emstar
//!
//! data = emstar.read("particles.star")
//! # SimpleBlock → dict, LoopBlock → dict of lists
//!
//! emstar.stats("particles.star")      # → dict
//! emstar.validate("particles.star")   # → None (raises on error)
//! emstar.write(data, "out.star")      # write back
//! ```

use crate::error::StarError;
use crate::star::{self, DataBlock, DataValue, LoopBlock, SimpleBlock};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Read a STAR file into a Python dict.
#[pyfunction]
fn read(path: &str) -> PyResult<PyObject> {
    let sf = star::read_file(path.as_ref()).map_err(to_pyerr)?;
    Python::with_gil(|py| {
        let dict = PyDict::new(py);
        for (name, block) in &sf.blocks {
            let inner = PyDict::new(py);
            match block {
                DataBlock::Simple(s) => {
                    for (k, v) in s.iter() {
                        inner.set_item(k, data_value_to_py(py, v))?;
                    }
                }
                DataBlock::Loop(l) => {
                    for (i, col) in l.col_names.iter().enumerate() {
                        let vals: Vec<PyObject> = l.col_data[i]
                            .iter()
                            .map(|dv| data_value_to_py(py, dv))
                            .collect();
                        inner.set_item(col.as_str(), vals)?;
                    }
                }
            }
            dict.set_item(name.as_str(), inner)?;
        }
        Ok(dict.into())
    })
}

/// Write a STAR file from a Python dict.
#[pyfunction]
fn write(data: PyObject, path: &str) -> PyResult<()> {
    Python::with_gil(|py| {
        let dict = data.bind(py).downcast::<PyDict>()?;
        let mut sf = star::StarFile::new();

        for (key, val) in dict.iter() {
            let name: String = key.extract()?;
            let inner = val.downcast::<PyDict>()?;

            // Detect whether values are lists → LoopBlock, else → SimpleBlock
            let mut has_list = false;
            let mut has_scalar = false;
            for v in inner.values() {
                if v.is_instance_of::<PyList>() {
                    has_list = true;
                } else {
                    has_scalar = true;
                }
            }

            if has_list && !has_scalar {
                let mut col_names = Vec::new();
                let mut col_data: Vec<Vec<DataValue>> = Vec::new();
                for (k, v) in inner.iter() {
                    let col_name: String = k.extract()?;
                    let lst = v.downcast::<PyList>()?;
                    let mut col = Vec::with_capacity(lst.len());
                    for item in lst.iter() {
                        col.push(py_to_data_value(&item));
                    }
                    col_names.push(col_name);
                    col_data.push(col);
                }
                sf.blocks
                    .push((name, DataBlock::Loop(LoopBlock { col_names, col_data })));
            } else {
                let mut entries = Vec::new();
                for (k, v) in inner.iter() {
                    let key: String = k.extract()?;
                    entries.push((key, py_to_data_value(&v)));
                }
                sf.blocks
                    .push((name, DataBlock::Simple(SimpleBlock { entries })));
            }
        }

        star::write_file(&sf, path.as_ref()).map_err(to_pyerr)
    })
}

/// Get statistics for a STAR file.
#[pyfunction]
fn stats(path: &str) -> PyResult<PyObject> {
    let sf = star::read_file(path.as_ref()).map_err(to_pyerr)?;
    let s = sf.stats();
    Python::with_gil(|py| {
        let d = PyDict::new(py);
        d.set_item("n_blocks", s.n_blocks)?;
        d.set_item("n_simple", s.n_simple)?;
        d.set_item("n_loop", s.n_loop)?;
        d.set_item("total_loop_rows", s.total_loop_rows)?;
        d.set_item("total_simple_entries", s.total_simple_entries)?;
        Ok(d.into())
    })
}

/// Validate a STAR file. Raises `ValueError` on invalid files.
#[pyfunction]
fn validate(path: &str) -> PyResult<()> {
    star::read_file(path.as_ref()).map_err(to_pyerr)?;
    Ok(())
}

fn py_to_data_value(obj: &Bound<'_, PyAny>) -> DataValue {
    if obj.is_none() {
        return DataValue::Null;
    }
    if let Ok(s) = obj.extract::<String>() {
        return DataValue::String(s);
    }
    if let Ok(i) = obj.extract::<i64>() {
        return DataValue::Integer(i);
    }
    if let Ok(f) = obj.extract::<f64>() {
        return DataValue::Float(f);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return DataValue::Bool(b);
    }
    DataValue::Null
}

fn data_value_to_py(py: Python<'_>, v: &DataValue) -> PyObject {
    match v {
        DataValue::Null => py.None(),
        DataValue::String(s) => s.clone().into_py(py),
        DataValue::Integer(i) => i.into_py(py),
        DataValue::Float(f) => f.into_py(py),
        DataValue::Bool(b) => b.into_py(py),
    }
}

fn to_pyerr(e: StarError) -> PyErr {
    PyValueError::new_err(e.to_string())
}

/// emstar — STAR file I/O for Python.
#[pymodule]
fn emstar(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(write, m)?)?;
    m.add_function(wrap_pyfunction!(stats, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    Ok(())
}
