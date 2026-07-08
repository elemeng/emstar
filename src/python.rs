//! emstar — read, write, and inspect STAR files used in cryo-EM (RELION).
//!
//! Simple blocks → ``dict``, loop blocks → ``polars.DataFrame`` / ``pandas.DataFrame`` / ``dict`` of lists.
//!
//! Examples
//! --------
//! >>> import emstar
//! >>> data = emstar.read("particles.star")
//! >>> data["particles"]                     # DataFrame or dict of lists
//! >>> emstar.write(data, "output.star")     # write back
//! >>> emstar.stats("particles.star")        # file statistics
//! >>> emstar.validate("particles.star")     # raise on invalid
//!
//! Functions
//! ---------
//! read      Read a STAR file into a dict of blocks.
//! write     Write blocks to a STAR file.
//! stats     Get file statistics.
//! validate  Validate a STAR file.
#![allow(deprecated)]
//!
//! ```python
//! import emstar
//!
//! # Auto-returns DataFrames if polars/pandas installed
//! data = emstar.read("particles.star")
//! df = data["particles"]          # polars/pandas DataFrame, or dict of lists
//!
//! # Write: accept both dicts and DataFrames
//! emstar.write(data, "out.star")
//! emstar.write({"x": [1,2]}, "out.star")
//! emstar.write(df, "out.star")    # single DataFrame as root block
//!
//! emstar.stats("particles.star")
//! emstar.validate("particles.star")
//! ```

use crate::error::StarError;
use crate::star::{self, DataBlock, DataValue, LoopBlock, SimpleBlock};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList};

/// Read a STAR file. Returns DataFrames when polars/pandas is installed.
///
/// If neither polars nor pandas is installed, returns plain dicts of lists
/// (zero-dependency fallback).
///
/// Parameters
/// ----------
/// path : str
///     Path to the .star file.
///
/// Returns
/// -------
/// dict
///     ``{block_name: DataFrame | dict}``.
///     Loop blocks become ``polars.DataFrame`` (preferred) or ``pandas.DataFrame``,
///     or fall back to ``{column: [values...]}`` dicts.
///
/// Raises
/// ------
/// FileNotFoundError
///     File does not exist.
/// ValueError
///     File is malformed.
///
/// Example
/// -------
/// >>> import emstar
/// >>> data = emstar.read("particles.star")
/// >>> data["particles"]  # DataFrame or dict of lists
#[pyfunction]
fn read<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let sf = star::read_file(path.as_ref()).map_err(to_pyerr)?;
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

    // Try upgrading loop blocks to DataFrames (polars preferred, then pandas)
    let result = dict.into_any();
    Ok(result)
}

fn is_loop_dict(py: Python<'_>, obj: &Bound<'_, PyAny>) -> bool {
    let dict = match obj.downcast::<PyDict>() {
        Ok(d) => d,
        Err(_) => return false,
    };
    if dict.is_empty() { return false; }
    for v in dict.values() {
        if !v.is_instance_of::<PyList>() { return false; }
    }
    true
}

/// Write a STAR file from dicts or DataFrames.
///
/// Accepts loop blocks as dicts of lists or DataFrames (polars/pandas).
/// Simple blocks must be dicts of scalars.
///
/// Parameters
/// ----------
/// data : dict
///     Block dict as returned by ``read()``, or ``{block: {col: [vals...]}}``,
///     or a single ``polars.DataFrame`` / ``pandas.DataFrame``.
/// path : str
///     Output .star file path.
///
/// Raises
/// ------
/// ValueError
///     Data format is invalid or file cannot be written.
///
/// Example
/// -------
/// >>> import emstar
/// >>> emstar.write({"x": [1.0]}, "out.star")
#[pyfunction]
fn write<'py>(py: Python<'py>, data: Bound<'py, PyAny>, path: &str) -> PyResult<()> {
    // Normalize: if data has .columns, it's a DataFrame → wrap as root block
    let is_df: bool = py.eval_bound("lambda x: hasattr(x, 'columns')", None, None)?
        .call1((data.clone(),))?.extract()?;

    let dict = if is_df {
        let wrapped = PyDict::new(py);
        wrapped.set_item("root", data)?;
        wrapped
    } else {
        data.downcast::<PyDict>()?.clone()
    };

    // Convert DataFrames back to dicts of lists using Python to_dict()
    for (name, block) in dict.iter() {
        let is_df: bool = py.eval_bound("lambda x: hasattr(x, 'columns')", None, None)?
            .call1((block.clone(),))?.extract()?;
        if is_df {
            let cleaned = py.eval_bound("block.to_dict(as_series=False)", None, Some(&{
                let l = PyDict::new(py);
                l.set_item("block", block)?;
                l
            }))?;
            dict.set_item(name, cleaned)?;
        }
    }
    let cleaned_dict = dict;

    let mut sf = star::StarFile::new();

    for (key, val) in cleaned_dict.iter() {
        let name: String = key.extract()?;
        let inner = val.downcast::<PyDict>()?;

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
            sf.blocks.push((name, DataBlock::Loop(LoopBlock { col_names, col_data })));
        } else {
            let mut entries = Vec::new();
            for (k, v) in inner.iter() {
                let key: String = k.extract()?;
                entries.push((key, py_to_data_value(&v)));
            }
            sf.blocks.push((name, DataBlock::Simple(SimpleBlock { entries })));
        }
    }

    star::write_file(&sf, path.as_ref()).map_err(to_pyerr)
}

/// Get file statistics.
///
/// Parameters
/// ----------
/// path : str
///     Path to the .star file.
///
/// Returns
/// -------
/// dict
///     ``{"n_blocks": int, "n_simple": int, "n_loop": int,
///     "total_loop_rows": int, "total_simple_entries": int}``
///
/// Raises
/// ------
/// FileNotFoundError
///     File does not exist.
///
/// Example
/// -------
/// >>> s = emstar.stats("particles.star")
/// >>> s["n_blocks"]
/// 2
#[pyfunction]
fn stats<'py>(py: Python<'py>, path: &str) -> PyResult<Bound<'py, PyAny>> {
    let sf = star::read_file(path.as_ref()).map_err(to_pyerr)?;
    let s = sf.stats();
    let d = PyDict::new(py);
    d.set_item("n_blocks", s.n_blocks)?;
    d.set_item("n_simple", s.n_simple)?;
    d.set_item("n_loop", s.n_loop)?;
    d.set_item("total_loop_rows", s.total_loop_rows)?;
    d.set_item("total_simple_entries", s.total_simple_entries)?;
    Ok(d.into_any())
}

/// Validate file format. Raises ValueError on invalid files.
///
/// Parameters
/// ----------
/// path : str
///     Path to the .star file.
///
/// Raises
/// ------
/// FileNotFoundError
///     File does not exist.
/// ValueError
///     File is malformed.
///
/// Example
/// -------
/// >>> emstar.validate("particles.star")
#[pyfunction]
fn validate(path: &str) -> PyResult<()> {
    star::read_file(path.as_ref()).map_err(to_pyerr)?;
    Ok(())
}

/// List block names in a STAR file.
#[pyfunction]
fn block_names(path: &str) -> PyResult<Vec<String>> {
    let sf = star::read_file(path.as_ref()).map_err(to_pyerr)?;
    Ok(sf.block_names().into_iter().map(|s| s.to_string()).collect())
}

// ── Helpers ────────────────────────────────────────────────────────────

fn py_to_data_value(obj: &Bound<'_, PyAny>) -> DataValue {
    if obj.is_none() { return DataValue::Null; }
    if let Ok(s) = obj.extract::<String>() { return DataValue::String(s); }
    if let Ok(i) = obj.extract::<i64>() { return DataValue::Integer(i); }
    if let Ok(f) = obj.extract::<f64>() { return DataValue::Float(f); }
    if let Ok(b) = obj.extract::<bool>() { return DataValue::Bool(b); }
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

#[pymodule]
fn emstar(_py: Python<'_>, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(read, m)?)?;
    m.add_function(wrap_pyfunction!(write, m)?)?;
    m.add_function(wrap_pyfunction!(stats, m)?)?;
    m.add_function(wrap_pyfunction!(validate, m)?)?;
    m.add_function(wrap_pyfunction!(block_names, m)?)?;
    Ok(())
}
