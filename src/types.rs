//! Data types for STAR file representation

use polars::prelude::*;
use smartstring::alias::String as SmartString;
use std::collections::HashMap;

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Represents a value in a STAR file
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DataValue {
    /// String value
    String(SmartString),
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// Boolean value
    Bool(bool),
    /// Null/NA value
    Null,
}

impl PartialOrd for DataValue {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Type-based ordering: Null < Bool < Integer < Float < String
        let type_order = |v: &DataValue| -> u8 {
            match v {
                DataValue::Null => 0,
                DataValue::Bool(_) => 1,
                DataValue::Integer(_) => 2,
                DataValue::Float(_) => 3,
                DataValue::String(_) => 4,
            }
        };

        let self_type = type_order(self);
        let other_type = type_order(other);

        if self_type != other_type {
            return self_type.partial_cmp(&other_type);
        }

        // Same type, compare values
        match (self, other) {
            (DataValue::Null, DataValue::Null) => Some(std::cmp::Ordering::Equal),
            (DataValue::Bool(a), DataValue::Bool(b)) => a.partial_cmp(b),
            (DataValue::Integer(a), DataValue::Integer(b)) => a.partial_cmp(b),
            // Float comparison: treat NaN as less than all other values for consistent ordering
            (DataValue::Float(a), DataValue::Float(b)) => {
                match (a.is_nan(), b.is_nan()) {
                    (true, true) => Some(std::cmp::Ordering::Equal),
                    (true, false) => Some(std::cmp::Ordering::Less),
                    (false, true) => Some(std::cmp::Ordering::Greater),
                    (false, false) => a.partial_cmp(b),
                }
            }
            (DataValue::String(a), DataValue::String(b)) => a.partial_cmp(b),
            _ => unreachable!(),
        }
    }
}

impl std::hash::Hash for DataValue {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        // Hash based on type discriminant and value
        // Note: Float values are hashed by their bit representation, which means NaN will hash consistently
        // even though NaN != NaN
        match self {
            DataValue::Null => 0u8.hash(state),
            DataValue::Bool(b) => {
                1u8.hash(state);
                b.hash(state);
            }
            DataValue::Integer(i) => {
                2u8.hash(state);
                i.hash(state);
            }
            DataValue::Float(f) => {
                3u8.hash(state);
                // Hash the bit representation to handle NaN consistently
                f.to_bits().hash(state);
            }
            DataValue::String(s) => {
                4u8.hash(state);
                s.hash(state);
            }
        }
    }
}

/// # Equality and NaN Handling
///
/// **Note on Float values and NaN:** Due to IEEE 754 semantics, `NaN != NaN`, which means
/// two `DataValue::Float(f64::NAN)` values will compare as unequal even though they
/// contain the same bit pattern. This violates the strict substitutability requirement
/// of `Eq` when NaN values are present.
///
/// For use in `HashMap` keys and similar collections:
/// - Avoid using DataValues containing NaN as keys
/// - If NaN values must be used as keys, be aware that `NaN != NaN` may cause unexpected behavior
/// - Consider normalizing NaN values to a canonical representation before use as keys
///
/// The `PartialEq` implementation correctly handles all other cases according to IEEE 754.
impl Eq for DataValue {}

impl DataValue {
    /// Try to convert to an integer
    #[inline]
    pub fn as_integer(&self) -> Option<i64> {
        match self {
            DataValue::Integer(i) => Some(*i),
            DataValue::Float(f) if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 => {
                Some(*f as i64)
            }
            _ => None,
        }
    }

    /// Try to convert to a float
    #[inline]
    pub fn as_float(&self) -> Option<f64> {
        match self {
            DataValue::Float(f) => Some(*f),
            DataValue::Integer(i) => Some(*i as f64),
            _ => None,
        }
    }

    /// Try to convert to a string
    #[inline]
    pub fn as_string(&self) -> Option<&str> {
        match self {
            DataValue::String(s) => Some(s.as_str()),
            _ => None,
        }
    }

    /// Try to convert to a boolean
    ///
    /// Accepts: true, false, "true", "false", "1", "0", "yes", "no" (case-insensitive)
    #[inline]
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            DataValue::Bool(b) => Some(*b),
            DataValue::Integer(i) => Some(*i != 0),
            DataValue::String(s) => match s.to_lowercase().as_str() {
                "true" | "yes" | "1" => Some(true),
                "false" | "no" | "0" => Some(false),
                _ => None,
            },
            _ => None,
        }
    }

    /// Check if value is null
    #[inline]
    pub fn is_null(&self) -> bool {
        matches!(self, DataValue::Null)
    }

    /// Check if value is NaN (only for Float variant)
    #[inline]
    pub fn is_nan(&self) -> bool {
        matches!(self, DataValue::Float(f) if f.is_nan())
    }

    /// Check if value is infinite (only for Float variant)
    #[inline]
    pub fn is_infinite(&self) -> bool {
        matches!(self, DataValue::Float(f) if f.is_infinite())
    }

    /// Get the type name of this value
    #[inline]
    pub fn type_name(&self) -> &'static str {
        match self {
            DataValue::String(_) => "String",
            DataValue::Integer(_) => "Integer",
            DataValue::Float(_) => "Float",
            DataValue::Bool(_) => "Bool",
            DataValue::Null => "Null",
        }
    }
}

// From implementations for DataValue
impl From<String> for DataValue {
    fn from(s: String) -> Self {
        DataValue::String(s.into())
    }
}

impl From<&str> for DataValue {
    fn from(s: &str) -> Self {
        DataValue::String(s.into())
    }
}

impl From<i32> for DataValue {
    fn from(i: i32) -> Self {
        DataValue::Integer(i as i64)
    }
}

impl From<i64> for DataValue {
    fn from(i: i64) -> Self {
        DataValue::Integer(i)
    }
}

impl From<u32> for DataValue {
    fn from(u: u32) -> Self {
        DataValue::Integer(u as i64)
    }
}

impl From<u64> for DataValue {
    fn from(u: u64) -> Self {
        DataValue::Integer(u as i64)
    }
}

impl From<f32> for DataValue {
    fn from(f: f32) -> Self {
        DataValue::Float(f as f64)
    }
}

impl From<f64> for DataValue {
    fn from(f: f64) -> Self {
        DataValue::Float(f)
    }
}

impl From<bool> for DataValue {
    fn from(b: bool) -> Self {
        DataValue::Bool(b)
    }
}

// TryFrom implementations for DataValue
impl TryFrom<DataValue> for i64 {
    type Error = crate::Error;

    fn try_from(value: DataValue) -> Result<Self, Self::Error> {
        value.as_integer()
            .ok_or_else(|| crate::Error::TypeConversion(format!("Cannot convert {:?} to i64", value)))
    }
}

impl TryFrom<DataValue> for f64 {
    type Error = crate::Error;

    fn try_from(value: DataValue) -> Result<Self, Self::Error> {
        value.as_float()
            .ok_or_else(|| crate::Error::TypeConversion(format!("Cannot convert {:?} to f64", value)))
    }
}

impl TryFrom<DataValue> for String {
    type Error = crate::Error;

    fn try_from(value: DataValue) -> Result<Self, Self::Error> {
        match value {
            DataValue::String(s) => Ok(s.into()),
            DataValue::Integer(i) => Ok(i.to_string()),
            DataValue::Float(f) => Ok(f.to_string()),
            DataValue::Bool(b) => Ok(b.to_string()),
            DataValue::Null => Err(crate::Error::TypeConversion("Cannot convert Null to String".to_string())),
        }
    }
}

impl TryFrom<DataValue> for bool {
    type Error = crate::Error;

    fn try_from(value: DataValue) -> Result<Self, Self::Error> {
        value.as_bool()
            .ok_or_else(|| crate::Error::TypeConversion(format!("Cannot convert {:?} to bool", value)))
    }
}

impl From<DataValue> for i32 {
    fn from(value: DataValue) -> Self {
        match value {
            DataValue::Integer(i) => i as i32,
            DataValue::Float(f) => f as i32,
            DataValue::Bool(b) => if b { 1 } else { 0 },
            DataValue::String(s) => s.parse().unwrap_or(i32::MIN),
            DataValue::Null => i32::MIN,
        }
    }
}

impl From<DataValue> for u32 {
    fn from(value: DataValue) -> Self {
        match value {
            DataValue::Integer(i) => i as u32,
            DataValue::Float(f) => f as u32,
            DataValue::Bool(b) => if b { 1 } else { 0 },
            DataValue::String(s) => s.parse().unwrap_or(u32::MAX),
            DataValue::Null => u32::MAX,
        }
    }
}

impl From<DataValue> for f32 {
    fn from(value: DataValue) -> Self {
        match value {
            DataValue::Integer(i) => i as f32,
            DataValue::Float(f) => f as f32,
            DataValue::Bool(b) => if b { 1.0 } else { 0.0 },
            DataValue::String(s) => s.parse().unwrap_or(f32::NAN),
            DataValue::Null => f32::NAN,
        }
    }
}

impl From<DataValue> for usize {
    fn from(value: DataValue) -> Self {
        match value {
            DataValue::Integer(i) => i as usize,
            DataValue::Float(f) => f as usize,
            DataValue::Bool(b) => if b { 1 } else { 0 },
            DataValue::String(s) => s.parse().unwrap_or(usize::MAX),
            DataValue::Null => usize::MAX,
        }
    }
}

// Display implementation for DataValue
impl std::fmt::Display for DataValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DataValue::String(s) => write!(f, "{}", s),
            DataValue::Integer(i) => write!(f, "{}", i),
            DataValue::Float(val) => write!(f, "{}", val),
            DataValue::Bool(b) => write!(f, "{}", b),
            DataValue::Null => write!(f, "<NA>"),
        }
    }
}

// FromStr implementation for DataValue
impl std::str::FromStr for DataValue {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let trimmed = s.trim();

        // Check for null/NA values
        if trimmed.eq_ignore_ascii_case("<NA>") || trimmed.eq_ignore_ascii_case("nan") {
            return Ok(DataValue::Null);
        }

        // Check for boolean values
        match trimmed.to_lowercase().as_str() {
            "true" | "yes" | "1" => return Ok(DataValue::Bool(true)),
            "false" | "no" | "0" => return Ok(DataValue::Bool(false)),
            _ => {}
        }

        // Try to parse as integer
        if let Ok(i) = trimmed.parse::<i64>() {
            return Ok(DataValue::Integer(i));
        }

        // Try to parse as float
        if let Ok(f) = trimmed.parse::<f64>() {
            return Ok(DataValue::Float(f));
        }

        // String value
        let unquoted = trimmed.trim_matches('"').trim_matches('\'');
        Ok(DataValue::String(unquoted.into()))
    }
}

/// Represents a simple (non-loop) data block
/// Contains key-value pairs
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SimpleBlock {
    /// Key-value pairs in the block
    data: HashMap<SmartString, DataValue>,
}

impl SimpleBlock {
    /// Create a new empty simple block
    #[inline]
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    /// Iterate over all key-value pairs in the block
    #[inline]
    pub fn iter(&self) -> impl Iterator<Item = (&str, &DataValue)> + '_ {
        self.data.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Get a value by key (Read)
    #[inline]
    pub fn get(&self, key: &str) -> Option<&DataValue> {
        self.data.get(key)
    }

    /// Set a value (Create/Update)
    #[inline]
    pub fn set(&mut self, key: &str, value: DataValue) {
        self.data.insert(key.into(), value);
    }

    /// Remove a key-value pair (Delete)
    #[inline]
    pub fn remove(&mut self, key: &str) -> Option<DataValue> {
        self.data.remove(key)
    }

    /// Check if a key exists
    #[inline]
    pub fn contains_key(&self, key: &str) -> bool {
        self.data.contains_key(key)
    }

    /// Get or insert a value by key
    ///
    /// Returns a mutable reference to the value for the given key.
    /// If the key doesn't exist, inserts DataValue::Null and returns a reference to it.
    ///
    /// # Arguments
    ///
    /// * `key` - The key to look up or insert
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::{SimpleBlock, DataValue};
    ///
    /// let mut block = SimpleBlock::new();
    /// let value = block.get_or_insert("new_key");
    /// *value = DataValue::Integer(42);
    /// assert_eq!(block.get("new_key"), Some(&DataValue::Integer(42)));
    /// ```
    #[inline]
    pub fn get_or_insert(&mut self, key: &str) -> &mut DataValue {
        self.data.entry(key.into()).or_insert_with(|| DataValue::Null)
    }

    /// Get all keys in the block
    pub fn keys(&self) -> impl Iterator<Item = &str> {
        self.data.keys().map(|k| k.as_str())
    }

    /// Get the number of key-value pairs
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Check if block is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Clear all key-value pairs (Delete all)
    #[inline]
    pub fn clear(&mut self) {
        self.data.clear();
    }

    /// Iterate over all values in the block
    pub fn values(&self) -> impl Iterator<Item = &DataValue> + '_ {
        self.data.values()
    }

    /// Retain only the entries specified by the predicate
    pub fn retain<F>(&mut self, mut f: F)
    where
        F: FnMut(&str, &DataValue) -> bool,
    {
        self.data.retain(|k, v| f(k.as_str(), v))
    }
    /// Clear the block, returning all key-value pairs as an iterator
    pub fn drain(&mut self) -> impl Iterator<Item = (String, DataValue)> + '_ {
        self.data.drain().map(|(k, v)| (k.into(), v))
    }

    /// Check if the block contains the given value
    pub fn contains_value(&self, value: &DataValue) -> bool {
        self.data.values().any(|v| v == value)
    }

    /// Get the first value (useful for blocks with single entry)
    pub fn first_value(&self) -> Option<&DataValue> {
        self.data.values().next()
    }
}

impl Default for SimpleBlock {
    fn default() -> Self {
        Self::new()
    }
}

impl FromIterator<(SmartString, DataValue)> for SimpleBlock {
    fn from_iter<T: IntoIterator<Item = (SmartString, DataValue)>>(iter: T) -> Self {
        Self {
            data: HashMap::from_iter(iter),
        }
    }
}

impl<const N: usize> From<[(&str, DataValue); N]> for SimpleBlock {
    fn from(items: [(&str, DataValue); N]) -> Self {
        Self {
            data: items.into_iter().map(|(k, v)| (k.into(), v)).collect(),
        }
    }
}

impl std::ops::Index<&str> for SimpleBlock {
    type Output = DataValue;

    fn index(&self, key: &str) -> &Self::Output {
        self.data.get(key).unwrap_or_else(|| {
            panic!(
                "Key '{}' not found in SimpleBlock (available keys: {:?})",
                key,
                self.data.keys().collect::<Vec<_>>()
            )
        })
    }
}

impl std::ops::IndexMut<&str> for SimpleBlock {
    fn index_mut(&mut self, key: &str) -> &mut Self::Output {
        self.data.entry(key.into()).or_insert_with(|| DataValue::Null)
    }
}

/// Column metadata for inspection without loading data
#[derive(Debug, Clone)]
pub struct ColumnMetadata {
    /// Column name
    pub name: String,
    /// Data type of the column
    pub dtype: DataType,
    /// Number of rows in the column
    pub len: usize,
    /// Number of null values in the column
    pub null_count: usize,
}

/// Represents a loop data block (table-like data)
/// Uses Polars DataFrame for efficient columnar storage and operations
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LoopBlock {
    /// Polars DataFrame containing the tabular data
    df: DataFrame,
}

impl LoopBlock {
    /// Create a new empty loop block
    #[inline]
    pub fn new() -> Self {
        Self {
            df: DataFrame::empty(),
        }
    }

    /// Create a builder for constructing a LoopBlock with a fluent API
    ///
    /// # Example
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let block = LoopBlock::builder()
    ///     .columns(&["col1", "col2"])
    ///     .rows(vec![
    ///         vec![DataValue::Float(1.0), DataValue::Float(2.0)],
    ///         vec![DataValue::Float(3.0), DataValue::Float(4.0)],
    ///     ])
    ///     .build()
    ///     .unwrap();
    ///
    /// assert_eq!(block.row_count(), 2);
    /// assert_eq!(block.column_count(), 2);
    /// ```
    #[inline]
    pub fn builder() -> LoopBlockBuilder {
        LoopBlockBuilder::new()
    }

    /// Create a LoopBlock from a Polars DataFrame
    #[inline]
    pub fn from_dataframe(df: DataFrame) -> Self {
        Self { df }
    }

    /// Create a LoopBlock with the given column names (all initially empty String type)
    ///
    /// # Arguments
    ///
    /// * `columns` - Slice of column names to create
    ///
    /// # Returns
    ///
    /// A new LoopBlock with the specified columns (all initially empty)
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::LoopBlock;
    ///
    /// let block = LoopBlock::with_columns(&["col1", "col2", "col3"]);
    /// assert_eq!(block.column_count(), 3);
    /// ```
    pub fn with_columns(columns: &[&str]) -> Self {
        let mut block = Self::new();
        for col in columns {
            block.add_column(col);
        }
        block
    }

    /// Get the underlying DataFrame
    #[inline]
    pub fn as_dataframe(&self) -> &DataFrame {
        &self.df
    }

    /// Get number of rows
    #[inline]
    pub fn row_count(&self) -> usize {
        self.df.height()
    }

    /// Get number of columns
    #[inline]
    pub fn column_count(&self) -> usize {
        self.df.width()
    }

    /// Get column names as a vector of string slices
    ///
    /// Returns a vector for convenience and to avoid lifetime issues. If you need
    /// an iterator, use `.iter()` on the result.
    ///
    /// # Returns
    ///
    /// Vector of column name references
    ///
    /// # Example
    ///
    /// ```ignore
    /// let cols = block.columns();
    /// for col in &cols {
    ///     println!("{}", col);
    /// }
    /// ```
    pub fn columns(&self) -> Vec<&str> {
        self.df.get_column_names().iter().map(|s| s.as_str()).collect()
    }

    /// Add a new empty column with the given name (String type by default)
    pub fn add_column(&mut self, name: &str) {
        let n_rows = self.row_count();
        let series: Series = if n_rows == 0 {
            Series::new(name.into(), Vec::<String>::new())
        } else {
            // Fill with nulls for existing rows
            let nulls: Vec<Option<String>> = vec![None; n_rows];
            Series::new(name.into(), nulls)
        };
        
        // Check if dataframe has no columns yet (empty dataframe with no schema)
        if self.df.width() == 0 {
            // Create new dataframe with just this column
            self.df = DataFrame::new(vec![series.into()]).unwrap_or_else(|_| DataFrame::empty());
        } else {
            // Add column to existing dataframe
            let _ = self.df.with_column(series);
        }
    }

    /// Add a row to the loop block
    pub fn add_row(&mut self, values: Vec<DataValue>) -> crate::Result<()> {
        let n_cols = self.column_count();
        let n_rows = self.row_count();
        
        // If no columns exist, create columns from values (first row defines structure)
        if n_cols == 0 {
            if values.is_empty() {
                return Ok(());
            }
            // Create columns from the first row
            for (i, value) in values.iter().enumerate() {
                let col_name = format!("col{}", i + 1);
                let series = single_value_to_series(&col_name, value)?;
                if i == 0 {
                    // Create initial dataframe
                    self.df = DataFrame::new(vec![series.into()])
                        .map_err(|e| crate::Error::InvalidFormat(format!("Failed to create column: {}", e)))?;
                } else {
                    // Add column to existing dataframe
                    let _ = self.df.with_column(series);
                }
            }
            return Ok(());
        }
        
        // Check column count matches
        if values.len() != n_cols {
            return Err(crate::Error::InvalidFormat(
                format!("Expected {} columns, got {}", n_cols, values.len())
            ));
        }
        
        // Get existing column names and types for type coercion
        let col_names = self.columns();
        let mut existing_dtypes: Vec<DataType> = Vec::with_capacity(col_names.len());
        for name in &col_names {
            let col = self.df.column(name)
                .map_err(|e| crate::Error::InvalidFormat(format!("Failed to get column '{}': {}", name, e)))?;
            existing_dtypes.push(col.dtype().clone());
        }
        
        // If no rows yet, rebuild the DataFrame with properly typed columns
        if n_rows == 0 {
            // Create new columns with proper types inferred from values
            let mut new_columns: Vec<Column> = Vec::new();
            for ((col_name, value), dtype) in col_names.iter().zip(values.iter()).zip(existing_dtypes.iter()) {
                // Infer the best type: if column is String but value is numeric, use numeric type
                let target_dtype = if matches!(dtype, DataType::String) {
                    match value {
                        DataValue::Integer(_) => DataType::Int64,
                        DataValue::Float(_) => DataType::Float64,
                        _ => DataType::String,
                    }
                } else {
                    dtype.clone()
                };
                
                let series = value_to_series_with_dtype(col_name, value, &target_dtype)?;
                new_columns.push(series.into());
            }
            // Replace the entire DataFrame
            self.df = DataFrame::new(new_columns)
                .map_err(|e| crate::Error::InvalidFormat(format!("Failed to create DataFrame: {}", e)))?;
            return Ok(());
        }
        
        // Convert values to a single-row DataFrame matching existing column types
        let mut new_columns: Vec<Column> = Vec::new();
        
        for ((col_name, value), dtype) in col_names.iter().zip(values.iter()).zip(existing_dtypes.iter()) {
            let series = value_to_series_with_dtype(col_name, value, dtype)?;
            new_columns.push(series.into());
        }
        
        let new_df = DataFrame::new(new_columns)
            .map_err(|e| crate::Error::InvalidFormat(format!("Failed to create row: {}", e)))?;
        
        self.df = self.df.vstack(&new_df)
            .map_err(|e| crate::Error::InvalidFormat(format!("Failed to add row: {}", e)))?;
        
        Ok(())
    }

    /// Get a value by row index and column index
    /// Returns DataValue (owned) since values are extracted from DataFrame
    pub fn get(&self, row_idx: usize, col_idx: usize) -> Option<DataValue> {
        let col_names = self.df.get_column_names();
        let col_name = col_names.get(col_idx)?;
        self.get_by_name(row_idx, col_name.as_str())
    }

    /// Get a value by row index and column name
    pub fn get_by_name(&self, row_idx: usize, col_name: &str) -> Option<DataValue> {
        let col = self.df.column(col_name).ok()?;

        // Check bounds
        if row_idx >= col.len() {
            return None;
        }

        Some(column_value_at(col, row_idx))
    }

    /// Get a f64 value by row index and column name
    ///
    /// Auto-converts Integer to Float if needed. Returns None if the value is null,
    /// the column doesn't exist, or the row index is out of bounds.
    ///
    /// # Arguments
    ///
    /// * `row_idx` - Row index (0-based)
    /// * `col_name` - Column name
    ///
    /// # Returns
    ///
    /// `Some(f64)` if the value exists and can be converted to float, `None` otherwise
    ///
    /// # Example
    ///
    /// ```ignore
    /// let x = block.get_f64(0, "rlnCoordinateX");
    /// ```
    #[inline]
    pub fn get_f64(&self, row_idx: usize, col_name: &str) -> Option<f64> {
        self.get_by_name(row_idx, col_name)?.as_float()
    }

    /// Get an i64 value by row index and column name
    ///
    /// Auto-converts Float to Integer if the value is a whole number. Returns None if
    /// the value is null, the column doesn't exist, or the row index is out of bounds.
    ///
    /// # Arguments
    ///
    /// * `row_idx` - Row index (0-based)
    /// * `col_name` - Column name
    ///
    /// # Returns
    ///
    /// `Some(i64)` if the value exists and can be converted to integer, `None` otherwise
    #[inline]
    pub fn get_i64(&self, row_idx: usize, col_name: &str) -> Option<i64> {
        self.get_by_name(row_idx, col_name)?.as_integer()
    }

    /// Get a string value by row index and column name
    ///
    /// Returns the string representation of any value type as SmartString. Returns None
    /// if the value is null, the column doesn't exist, or the row index is out of bounds.
    ///
    /// # Arguments
    ///
    /// * `row_idx` - Row index (0-based)
    /// * `col_name` - Column name
    ///
    /// # Returns
    ///
    /// `Some(SmartString)` if the value exists, `None` otherwise
    #[inline]
    pub fn get_string(&self, row_idx: usize, col_name: &str) -> Option<SmartString> {
        match self.get_by_name(row_idx, col_name)? {
            DataValue::String(s) => Some(s),
            DataValue::Integer(i) => Some(i.to_string().into()),
            DataValue::Float(f) => Some(f.to_string().into()),
            DataValue::Bool(b) => Some(b.to_string().into()),
            DataValue::Null => None,
        }
    }

    /// Set a value by row and column index
    ///
    /// # Performance Note
    /// This operation is O(n) where n is the number of rows in the column,
    /// as Polars DataFrames use immutable columnar storage. The entire column
    /// must be recreated to change a single value. For batch updates to an
    /// entire row, use [`Self::update_row`] which minimizes overhead.
    ///
    /// # Arguments
    ///
    /// * `row_idx` - Row index (0-based)
    /// * `col_idx` - Column index (0-based)
    /// * `value` - New value to set
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidFormat`] if the column index is out of bounds.
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let mut block = LoopBlock::with_columns(&["col1", "col2"]);
    /// block.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)]).unwrap();
    ///
    /// block.set(0, 0, DataValue::Integer(10)).unwrap();
    /// assert_eq!(block.get(0, 0), Some(DataValue::Integer(10)));
    /// ```
    pub fn set(&mut self, row_idx: usize, col_idx: usize, value: DataValue) -> Result<(), crate::Error> {
        let col_name = self.df.get_column_names()
            .get(col_idx)
            .map(|s| s.as_str().to_string())
            .ok_or_else(|| crate::Error::InvalidFormat(format!("Column index {} out of bounds", col_idx)))?;
        self.set_by_name(row_idx, &col_name, value)
    }

    /// Set a value by row index and column name
    /// 
    /// # Performance Warning
    /// This operation is **O(n)** where n is the number of rows in the column,
    /// as Polars DataFrames use immutable columnar storage. The entire column
    /// must be recreated to change a single value.
    /// 
    /// **Avoid calling this in a loop** for multiple updates. Instead:
    /// - For updating an entire row: Use [`Self::update_row`]
    /// - For batch updates across multiple rows/columns: Collect changes and 
    ///   rebuild the LoopBlock using the builder pattern
    /// 
    /// # Example
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let mut block = LoopBlock::with_columns(&["x", "y"]);
    /// block.add_row(vec![DataValue::Float(1.0), DataValue::Float(2.0)]).unwrap();
    ///
    /// // Single update - OK
    /// block.set_by_name(0, "x", DataValue::Float(10.0)).unwrap();
    /// ```
    pub fn set_by_name(&mut self, row_idx: usize, col_name: &str, value: DataValue) -> Result<(), crate::Error> {
        // Check if column exists first
        if !self.df.get_column_names().iter().any(|&c| c.as_str() == col_name) {
            let available_cols = self.columns();
            return Err(crate::Error::InvalidFormat(format!(
                "Column '{}' not found. Available columns: {:?}",
                col_name,
                available_cols
            )));
        }

        let column = self.df.column(col_name)
            .map_err(|e| crate::Error::InvalidFormat(format!("Failed to get column: {}", e)))?;

        let new_series = data_values_to_series_with_replacement(column, row_idx, value)?;
        self.df.replace(col_name, new_series)
            .map_err(|e| crate::Error::InvalidFormat(format!("Failed to set value: {}", e)))?;
        Ok(())
    }

    /// Check if block is empty
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.df.is_empty()
    }

    /// Get a full column as a vector of DataValues (Read)
    ///
    /// # Performance Note
    /// This operation is O(n) where n is the number of rows, and clones all values.
    /// For iteration without cloning, use [`column_iter_f64()`], [`column_iter_i64()`],
    /// or [`column_iter_str()`] which return iterators over borrowed values.
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let mut block = LoopBlock::with_columns(&["col1"]);
    /// block.add_row(vec![DataValue::Integer(1)]).unwrap();
    /// block.add_row(vec![DataValue::Integer(2)]).unwrap();
    ///
    /// let column = block.get_column("col1").unwrap();
    /// assert_eq!(column.len(), 2);
    /// ```
    pub fn get_column(&self, col_name: &str) -> Option<Vec<DataValue>> {
        let col = self.df.column(col_name).ok()?;
        let dtype = col.dtype();
        
        match dtype {
            DataType::Float64 => {
                let ca = col.f64().ok()?;
                Some(ca.into_iter().map(|opt| match opt {
                    Some(v) => DataValue::Float(v),
                    None => DataValue::Null,
                }).collect())
            }
            DataType::Int64 => {
                let ca = col.i64().ok()?;
                Some(ca.into_iter().map(|opt| match opt {
                    Some(v) => DataValue::Integer(v),
                    None => DataValue::Null,
                }).collect())
            }
            DataType::String => {
                let ca = col.str().ok()?;
                Some(ca.into_iter().map(|opt| match opt {
                    Some(v) => DataValue::String(v.into()),
                    None => DataValue::Null,
                }).collect())
            }
            _ => {
                // Fallback
                Some((0..col.len())
                    .map(|i| col.get(i).ok().map(|av| {
                        if av.is_null() {
                            DataValue::Null
                        } else {
                            DataValue::String(av.to_string().into())
                        }
                    }).unwrap_or(DataValue::Null))
                    .collect())
            }
        }
    }

    /// Remove a row by index (Delete)
    pub fn remove_row(&mut self, row_idx: usize) -> crate::Result<()> {
        // Check bounds
        if row_idx >= self.row_count() {
            return Err(crate::Error::InvalidFormat(
                format!("Row index {} out of bounds (total rows: {})", row_idx, self.row_count())
            ));
        }
        
        // Create mask to filter out the row
        let mask: Vec<bool> = (0..self.row_count())
            .map(|i| i != row_idx)
            .collect();
        
        let filter_series = Series::new("filter".into(), mask);
        let mask_bool = filter_series.bool().map_err(|e| 
            crate::Error::InvalidFormat(format!("Failed to create filter: {}", e))
        )?;
        self.df = self.df.filter(mask_bool).map_err(|e| 
            crate::Error::InvalidFormat(format!("Failed to remove row: {}", e))
        )?;
        
        Ok(())
    }

    /// Remove a column by name (Delete)
    pub fn remove_column(&mut self, col_name: &str) -> crate::Result<()> {
        self.df.drop_in_place(col_name).map_err(|e| 
            crate::Error::InvalidFormat(format!("Failed to remove column '{}': {}", col_name, e))
        )?;
        Ok(())
    }

    /// Clear all rows but keep columns (Delete all rows)
    pub fn clear_rows(&mut self) {
        // Use slice to keep schema but have 0 rows
        self.df = self.df.slice(0, 0);
    }

    /// Clear all data including columns (Delete all)
    pub fn clear(&mut self) {
        self.df = DataFrame::empty();
    }

    /// Iterate over rows, returning `Vec<DataValue>` for each row
    ///
    /// # Performance Note
    /// This operation is O(n*m) where n is the number of rows and m is the number of columns.
    /// Each iteration creates a new vector and clones all values in the row. For better
    /// performance with large datasets, consider using column iterators instead.
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let mut block = LoopBlock::with_columns(&["col1", "col2"]);
    /// block.add_row(vec![DataValue::Integer(1), DataValue::Integer(2)]).unwrap();
    ///
    /// for row in block.iter_rows() {
    ///     println!("Row: {:?}", row);
    /// }
    /// ```
    pub fn iter_rows(&self) -> impl Iterator<Item = Vec<DataValue>> + '_ {
        let nrows = self.row_count();
        let columns: Vec<_> = self.df.get_columns().iter().collect();
        
        (0..nrows).map(move |row_idx| {
            columns.iter()
                .map(|col| column_value_at(col, row_idx))
                .collect()
        })
    }

    /// Iterate over a column's values as f64
    /// Returns an iterator that yields Some(f64) for numeric values or None for nulls
    pub fn column_iter_f64(&self, col_name: &str) -> Option<impl Iterator<Item = Option<f64>> + '_> {
        let col = self.df.column(col_name).ok()?;
        let ca = col.f64().ok()?;
        Some(ca.into_iter())
    }

    /// Iterate over a column's values as i64
    /// Returns an iterator that yields Some(i64) for integer values or None for nulls
    pub fn column_iter_i64(&self, col_name: &str) -> Option<impl Iterator<Item = Option<i64>> + '_> {
        let col = self.df.column(col_name).ok()?;
        let ca = col.i64().ok()?;
        Some(ca.into_iter())
    }

    /// Iterate over a column's values as strings
    /// Returns an iterator that yields Some(&str) for string values or None for nulls
    pub fn column_iter_str(&self, col_name: &str) -> Option<impl Iterator<Item = Option<&str>> + '_> {
        let col = self.df.column(col_name).ok()?;
        let ca = col.str().ok()?;
        Some(ca.into_iter())
    }

    /// Get column metadata without loading data
    ///
    /// Returns metadata about a column including its type, length, and null count.
    /// This is useful for making decisions about data processing without loading the actual data.
    ///
    /// # Arguments
    ///
    /// * `col_name` - Column name
    ///
    /// # Returns
    ///
    /// `Some(ColumnMetadata)` if column exists, `None` otherwise
    pub fn column_metadata(&self, col_name: &str) -> Option<ColumnMetadata> {
        let col = self.df.column(col_name).ok()?;
        Some(ColumnMetadata {
            name: col_name.to_string(),
            dtype: col.dtype().clone(),
            len: col.len(),
            null_count: col.null_count(),
        })
    }

}

impl Default for LoopBlock {
    fn default() -> Self {
        Self::new()
    }
}

/// Builder for constructing LoopBlock with a fluent API
///
/// # Example
/// ```
/// use emstar::{LoopBlock, DataValue};
///
/// let block = LoopBlock::builder()
///     .columns(&["rlnCoordinateX", "rlnCoordinateY"])
///     .rows(vec![
///         vec![DataValue::Float(100.0), DataValue::Float(200.0)],
///         vec![DataValue::Float(150.0), DataValue::Float(250.0)],
///     ])
///     .build()
///     .unwrap();
/// ```
#[derive(Debug)]
pub struct LoopBlockBuilder {
    columns: Vec<String>,
    rows: Vec<Vec<DataValue>>,
}

impl LoopBlockBuilder {
    /// Create a new builder
    #[inline]
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Set the column names
    ///
    /// # Example
    /// ```
    /// use emstar::LoopBlock;
    ///
    /// let block = LoopBlock::builder()
    ///     .columns(&["col1", "col2", "col3"])
    ///     .build()
    ///     .unwrap();
    ///
    /// assert_eq!(block.column_count(), 3);
    /// ```
    pub fn columns(mut self, columns: &[&str]) -> Self {
        self.columns = columns.iter().map(|&s| s.to_string()).collect();
        self
    }

    /// Set all rows at once
    ///
    /// # Example
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let block = LoopBlock::builder()
    ///     .columns(&["col1", "col2"])
    ///     .rows(vec![
    ///         vec![DataValue::Float(1.0), DataValue::Float(2.0)],
    ///         vec![DataValue::Float(3.0), DataValue::Float(4.0)],
    ///     ])
    ///     .build()
    ///     .unwrap();
    ///
    /// assert_eq!(block.row_count(), 2);
    /// ```
    pub fn rows(mut self, rows: Vec<Vec<DataValue>>) -> Self {
        self.rows = rows;
        self
    }

    /// Validate the current builder state (useful for early error detection)
    ///
    /// Returns an error if the builder state is invalid, such as having rows
    /// without columns or rows with incorrect column counts.
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let builder = LoopBlock::builder()
    ///     .columns(&["col1", "col2"])
    ///     .rows(vec![vec![DataValue::Integer(1)]]);
    ///
    /// // This will fail because the row has only 1 column but 2 were defined
    /// assert!(builder.validate().is_err());
    /// ```
    pub fn validate(&self) -> Result<(), crate::Error> {
        if self.columns.is_empty() && !self.rows.is_empty() {
            return Err(crate::Error::InvalidFormat(
                "Cannot have rows without columns".to_string()
            ));
        }

        let expected_cols = self.columns.len();
        for (i, row) in self.rows.iter().enumerate() {
            if row.len() != expected_cols {
                return Err(crate::Error::InvalidFormat(format!(
                    "Row {} has {} columns, expected {}",
                    i, row.len(), expected_cols
                )));
            }
        }

        Ok(())
    }

    /// Build the LoopBlock with immediate validation
    ///
    /// This is a convenience method that calls validate() then build().
    /// Returns an error if the builder state is invalid or if the row data
    /// doesn't match the column count.
    ///
    /// # Example
    ///
    /// ```
    /// use emstar::{LoopBlock, DataValue};
    ///
    /// let block = LoopBlock::builder()
    ///     .columns(&["col1", "col2"])
    ///     .rows(vec![vec![DataValue::Integer(1), DataValue::Integer(2)]])
    ///     .build_validated()
    ///     .unwrap();
    /// ```
    pub fn build_validated(self) -> crate::Result<LoopBlock> {
        self.validate()?;
        self.build()
    }

    /// Build the LoopBlock
    ///
    /// Returns an error if the row data doesn't match the column count.
    pub fn build(self) -> crate::Result<LoopBlock> {
        let mut block = LoopBlock::new();

        // Add columns if specified (take ownership to avoid cloning)
        for col in self.columns {
            block.add_column(&col);
        }

        // Add rows (take ownership to avoid cloning)
        for row in self.rows {
            block.add_row(row)?;
        }

        Ok(block)
    }
}

impl Default for LoopBlockBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl PartialEq for LoopBlock {
    fn eq(&self, other: &Self) -> bool {
        // Compare DataFrames by checking if they're equal
        // This is a simplified comparison - proper DataFrame comparison would be more complex
        self.columns() == other.columns() && self.row_count() == other.row_count()
    }
}

/// Statistics for a LoopBlock
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LoopBlockStats {
    /// Number of rows
    pub n_rows: usize,
    /// Number of columns
    pub n_cols: usize,
    /// Total number of cells (rows * columns)
    pub n_cells: usize,
}

impl LoopBlock {
    /// Get statistics for this loop block
    #[inline]
    pub fn stats(&self) -> LoopBlockStats {
        let n_rows = self.row_count();
        let n_cols = self.column_count();
        LoopBlockStats {
            n_rows,
            n_cols,
            n_cells: n_rows * n_cols,
        }
    }
}

/// Statistics for a SimpleBlock
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SimpleBlockStats {
    /// Number of key-value pairs
    pub n_entries: usize,
}

impl SimpleBlock {
    /// Get statistics for this simple block
    #[inline]
    pub fn stats(&self) -> SimpleBlockStats {
        SimpleBlockStats {
            n_entries: self.len(),
        }
    }
}

/// Statistics for a DataBlock (either Simple or Loop)
#[derive(Debug, Clone, Copy, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DataBlockStats {
    /// Statistics for a simple block
    Simple(SimpleBlockStats),
    /// Statistics for a loop block
    Loop(LoopBlockStats),
}

/// Comprehensive statistics for a STAR file
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct StarStats {
    /// Total number of data blocks
    pub n_blocks: usize,
    /// Number of SimpleBlock data blocks
    pub n_simple_blocks: usize,
    /// Number of LoopBlock data blocks
    pub n_loop_blocks: usize,
    /// Total number of rows across all LoopBlocks
    pub total_loop_rows: usize,
    /// Total number of columns across all LoopBlocks (sum of each block's columns)
    pub total_loop_cols: usize,
    /// Total number of entries across all SimpleBlocks
    pub total_simple_entries: usize,
    /// Statistics per block (block name -> stats)
    pub block_stats: Vec<(String, DataBlockStats)>,
}

impl StarStats {
    /// Create a new StarStats from a HashMap of data blocks
    pub fn from_blocks(blocks: &HashMap<String, DataBlock>) -> Self {
        let mut n_simple_blocks = 0;
        let mut n_loop_blocks = 0;
        let mut total_loop_rows = 0;
        let mut total_loop_cols = 0;
        let mut total_simple_entries = 0;
        let mut block_stats = Vec::new();

        for (name, block) in blocks {
            let stats = block.stats();
            match &stats {
                DataBlockStats::Simple(s) => {
                    n_simple_blocks += 1;
                    total_simple_entries += s.n_entries;
                }
                DataBlockStats::Loop(l) => {
                    n_loop_blocks += 1;
                    total_loop_rows += l.n_rows;
                    total_loop_cols += l.n_cols;
                }
            }
            block_stats.push((name.clone(), stats));
        }

        // Sort block stats by name for consistent output
        block_stats.sort_by(|a, b| a.0.cmp(&b.0));

        StarStats {
            n_blocks: blocks.len(),
            n_simple_blocks,
            n_loop_blocks,
            total_loop_rows,
            total_loop_cols,
            total_simple_entries,
            block_stats,
        }
    }

    /// Get statistics for a specific block by name
    #[inline]
    pub fn get_block_stats(&self, name: &str) -> Option<DataBlockStats> {
        self.block_stats
            .iter()
            .find(|(n, _)| n == name)
            .map(|(_, stats)| *stats)
    }

    /// Returns true if the file contains any LoopBlocks
    #[inline]
    pub fn has_loop_blocks(&self) -> bool {
        self.n_loop_blocks > 0
    }

    /// Returns true if the file contains any SimpleBlocks
    #[inline]
    pub fn has_simple_blocks(&self) -> bool {
        self.n_simple_blocks > 0
    }

}

impl DataBlock {
    /// Get the name of the block type
    #[inline]
    pub fn block_type(&self) -> &'static str {
        match self {
            DataBlock::Simple(_) => "SimpleBlock",
            DataBlock::Loop(_) => "LoopBlock",
        }
    }

    /// Check if this is a simple block
    #[inline]
    pub fn is_simple(&self) -> bool {
        matches!(self, DataBlock::Simple(_))
    }

    /// Check if this is a loop block
    #[inline]
    pub fn is_loop(&self) -> bool {
        matches!(self, DataBlock::Loop(_))
    }

    /// Get as simple block if applicable
    #[inline]
    pub fn as_simple(&self) -> Option<&SimpleBlock> {
        match self {
            DataBlock::Simple(block) => Some(block),
            _ => None,
        }
    }

    /// Get as loop block if applicable
    #[inline]
    pub fn as_loop(&self) -> Option<&LoopBlock> {
        match self {
            DataBlock::Loop(block) => Some(block),
            _ => None,
        }
    }

    /// Get mutable reference to simple block if applicable
    #[inline]
    pub fn as_simple_mut(&mut self) -> Option<&mut SimpleBlock> {
        match self {
            DataBlock::Simple(block) => Some(block),
            _ => None,
        }
    }

    /// Get mutable reference to loop block if applicable
    #[inline]
    pub fn as_loop_mut(&mut self) -> Option<&mut LoopBlock> {
        match self {
            DataBlock::Loop(block) => Some(block),
            _ => None,
        }
    }

    /// Get a reference to the SimpleBlock, panicking with the given message if not a SimpleBlock
    #[inline]
    pub fn expect_simple(&self, msg: &str) -> &SimpleBlock {
        match self {
            DataBlock::Simple(block) => block,
            _ => panic!("{}", msg),
        }
    }

    /// Get a reference to the LoopBlock, panicking with the given message if not a LoopBlock
    #[inline]
    pub fn expect_loop(&self, msg: &str) -> &LoopBlock {
        match self {
            DataBlock::Loop(block) => block,
            _ => panic!("{}", msg),
        }
    }

    /// Get statistics for this data block
    #[inline]
    pub fn stats(&self) -> DataBlockStats {
        match self {
            DataBlock::Simple(block) => DataBlockStats::Simple(block.stats()),
            DataBlock::Loop(block) => DataBlockStats::Loop(block.stats()),
        }
    }

    /// Get the number of rows (for LoopBlock) or entries (for SimpleBlock)
    #[inline]
    pub fn count(&self) -> usize {
        match self {
            DataBlock::Simple(block) => block.len(),
            DataBlock::Loop(block) => block.row_count(),
        }
    }
}

/// Helper function to extract a DataValue from a column at a specific row
#[inline]
fn column_value_at(column: &Column, row_idx: usize) -> DataValue {
    match column.dtype() {
        DataType::Float64 => {
            column.f64()
                .ok()
                .and_then(|ca| ca.get(row_idx))
                .map(DataValue::Float)
                .unwrap_or(DataValue::Null)
        }
        DataType::Int64 => {
            column.i64()
                .ok()
                .and_then(|ca| ca.get(row_idx))
                .map(DataValue::Integer)
                .unwrap_or(DataValue::Null)
        }
        DataType::String => {
            column.str()
                .ok()
                .and_then(|ca| ca.get(row_idx))
                .map(|s| DataValue::String(s.into()))
                .unwrap_or(DataValue::Null)
        }
        _ => {
            column.get(row_idx)
                .ok()
                .map(|av| {
                    if av.is_null() {
                        DataValue::Null
                    } else {
                        DataValue::String(av.to_string().into())
                    }
                })
                .unwrap_or(DataValue::Null)
        }
    }
}

/// Helper function to create a new series with a single value replaced
///
/// Note: This operation is O(n) where n is the column length, as it requires
/// recreating the entire column. For batch updates, consider using Polars expressions
/// or rebuilding the DataFrame from scratch.
fn data_values_to_series_with_replacement(column: &Column, row_idx: usize, new_value: DataValue) -> Result<Series, crate::Error> {
    let dtype = column.dtype();
    let name = column.name();
    let len = column.len();

    // Pre-allocate with exact capacity
    match dtype {
        DataType::Float64 => {
            let ca = column.f64().map_err(|e| crate::Error::InvalidFormat(format!("Failed to get float values: {}", e)))?;
            let mut values: Vec<Option<f64>> = Vec::with_capacity(len);
            values.extend(ca.into_iter().enumerate().map(|(i, v)| {
                if i == row_idx {
                    data_value_to_option_f64(&new_value)
                } else {
                    v
                }
            }));
            Ok(Series::new(name.clone(), values))
        }
        DataType::Int64 => {
            let ca = column.i64().map_err(|e| crate::Error::InvalidFormat(format!("Failed to get int values: {}", e)))?;
            let mut values: Vec<Option<i64>> = Vec::with_capacity(len);
            values.extend(ca.into_iter().enumerate().map(|(i, v)| {
                if i == row_idx {
                    data_value_to_option_i64(&new_value)
                } else {
                    v
                }
            }));
            Ok(Series::new(name.clone(), values))
        }
        DataType::String => {
            let ca = column.str().map_err(|e| crate::Error::InvalidFormat(format!("Failed to get string values: {}", e)))?;
            let mut values: Vec<Option<String>> = Vec::with_capacity(len);
            values.extend(ca.into_iter().enumerate().map(|(i, v)| {
                if i == row_idx {
                    data_value_to_option_string(&new_value)
                } else {
                    v.map(|s| s.to_string())
                }
            }));
            Ok(Series::new(name.clone(), values))
        }
        _ => {
            // Fallback: convert to string series
            let mut values: Vec<Option<String>> = Vec::with_capacity(len);
            for i in 0..len {
                let val = if i == row_idx {
                    Some(new_value.as_string().unwrap_or("<NA>").to_string())
                } else {
                    column.get(i).ok().map(|av| av.to_string())
                };
                values.push(val);
            }
            Ok(Series::new(name.clone(), values))
        }
    }
}

/// Helper function to convert a single DataValue to a one-element Series
fn single_value_to_series(name: &str, value: &DataValue) -> Result<Series, crate::Error> {
    let series = match value {
        DataValue::Integer(i) => Series::new(name.into(), vec![*i]),
        DataValue::Float(f) => Series::new(name.into(), vec![*f]),
        DataValue::String(s) => Series::new(name.into(), vec![s.as_str()]),
        DataValue::Bool(b) => Series::new(name.into(), vec![*b]),
        DataValue::Null => {
            // Create a null string series by default
            let nulls: Vec<Option<String>> = vec![None];
            Series::new(name.into(), nulls)
        }
    };
    Ok(series)
}

/// Helper function to convert a DataValue to a Series with a specific dtype
fn value_to_series_with_dtype(name: &str, value: &DataValue, dtype: &DataType) -> Result<Series, crate::Error> {
    let series = match dtype {
        DataType::Float64 => {
            let val = data_value_to_option_f64(value);
            Series::new(name.into(), vec![val])
        }
        DataType::Int64 => {
            let val = data_value_to_option_i64(value);
            Series::new(name.into(), vec![val])
        }
        DataType::String => {
            let val = data_value_to_option_string(value);
            Series::new(name.into(), vec![val])
        }
        DataType::Boolean => {
            let val = match value {
                DataValue::Bool(b) => Some(*b),
                DataValue::Null => None,
                _ => data_value_to_option_string(value)
                    .and_then(|s| match s.as_str() {
                        "true" | "yes" | "1" => Some(true),
                        "false" | "no" | "0" => Some(false),
                        _ => None,
                    }),
            };
            Series::new(name.into(), vec![val])
        }
        _ => {
            // Fallback to string
            let val = data_value_to_option_string(value);
            Series::new(name.into(), vec![val])
        }
    };
    Ok(series)
}

/// Unified helper to convert DataValue to Option<f64>
fn data_value_to_option_f64(value: &DataValue) -> Option<f64> {
    match value {
        DataValue::Float(f) => Some(*f),
        DataValue::Integer(i) => Some(*i as f64),
        DataValue::Bool(b) => Some(if *b { 1.0 } else { 0.0 }),
        DataValue::Null => None,
        DataValue::String(s) => s.parse::<f64>().ok(),
    }
}

/// Unified helper to convert DataValue to Option<i64>
fn data_value_to_option_i64(value: &DataValue) -> Option<i64> {
    match value {
        DataValue::Integer(i) => Some(*i),
        DataValue::Float(f) if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 => Some(*f as i64),
        DataValue::Float(_) => None, // Float that can't be converted to i64
        DataValue::Bool(b) => Some(if *b { 1 } else { 0 }),
        DataValue::Null => None,
        DataValue::String(s) => s.parse::<i64>().ok(),
    }
}

/// Unified helper to convert DataValue to Option<String>
fn data_value_to_option_string(value: &DataValue) -> Option<String> {
    match value {
        DataValue::String(s) => Some(s.to_string()),
        DataValue::Integer(i) => Some(i.to_string()),
        DataValue::Float(f) => Some(f.to_string()),
        DataValue::Bool(b) => Some(b.to_string()),
        DataValue::Null => None,
    }
}

/// Represents a data block in a STAR file
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum DataBlock {
    /// Simple block with key-value pairs
    Simple(SimpleBlock),
    /// Loop block with tabular data
    Loop(LoopBlock),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_nan_ordering() {
        // NaN should be consistently ordered (less than all other values)
        let nan = DataValue::Float(f64::NAN);
        let num = DataValue::Float(1.0);
        let null = DataValue::Null;
        
        // NaN == NaN for ordering purposes
        assert_eq!(nan.partial_cmp(&nan), Some(std::cmp::Ordering::Equal));
        
        // NaN < any number
        assert_eq!(nan.partial_cmp(&num), Some(std::cmp::Ordering::Less));
        assert_eq!(num.partial_cmp(&nan), Some(std::cmp::Ordering::Greater));
        
        // Null < NaN (Null has lowest type order)
        assert_eq!(null.partial_cmp(&nan), Some(std::cmp::Ordering::Less));
        assert_eq!(nan.partial_cmp(&null), Some(std::cmp::Ordering::Greater));
    }

    #[test]
    fn test_simpleblock_index_panic_message() {
        let block = SimpleBlock::new();
        let result = std::panic::catch_unwind(|| {
            let _ = &block["nonexistent_key"];
        });
        assert!(result.is_err());
        // The panic message should include the key name
    }

    #[test]
    fn test_data_value_conversions() {
        let int_val = DataValue::Integer(42);
        assert_eq!(int_val.as_integer(), Some(42));
        assert_eq!(int_val.as_float(), Some(42.0));

        let float_val = DataValue::Float(3.14);
        assert_eq!(float_val.as_float(), Some(3.14));
        assert_eq!(float_val.as_integer(), None);

        let string_val = DataValue::String("hello".into());
        assert_eq!(string_val.as_string(), Some("hello"));
        assert_eq!(string_val.as_integer(), None);
        assert_eq!(string_val.as_float(), None);
    }

    #[test]
    fn test_loop_block() {
        // Create DataFrame with test data
        let s1 = Series::new("col1".into(), &[1i64]);
        let s2 = Series::new("col2".into(), &[2i64]);
        let df = DataFrame::new(vec![s1.into(), s2.into()]).unwrap();
        let block = LoopBlock::from_dataframe(df);

        assert_eq!(block.column_count(), 2);
        assert_eq!(block.row_count(), 1);
        assert_eq!(block.get_by_name(0, "col1"), Some(DataValue::Integer(1)));
        assert_eq!(block.get_by_name(0, "col2"), Some(DataValue::Integer(2)));
    }
}