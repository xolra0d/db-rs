use serde::{Deserialize, Serialize};
use sqlparser::ast::{DataType as SQLDatatype, Value as SQLValue};
use std::cmp::Ordering;
use uuid::Uuid;

use crate::error::{Error, Result};

/// Represents a parsed value in our custom protocol
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
    Null,
    String(String),
    Uuid(Uuid),
    Bool(bool),

    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),

    UInt8(u8),
    UInt16(u16),
    UInt32(u32),
    UInt64(u64),
}

impl TryFrom<(SQLValue, &ValueType)> for Value {
    type Error = Error;
    fn try_from(value: (SQLValue, &ValueType)) -> Result<Self> {
        let (sql_value, value_type) = value;

        match sql_value {
            SQLValue::Null => Ok(Self::Null),
            SQLValue::SingleQuotedString(string)
            | SQLValue::TripleSingleQuotedString(string)
            | SQLValue::TripleDoubleQuotedString(string) => {
                if value_type == &ValueType::String {
                    Ok(Self::String(string))
                } else if value_type == &ValueType::Uuid {
                    let uuid = Uuid::parse_str(&string).map_err(|_| Error::InvalidSource)?;
                    Ok(Self::Uuid(uuid))
                } else {
                    Err(Error::InvalidSource)
                }
            }
            SQLValue::Number(number, _) => {
                let parse_err = |_| Error::InvalidSource;
                match value_type {
                    ValueType::Int8 => Ok(Self::Int8(number.parse().map_err(parse_err)?)),
                    ValueType::Int16 => Ok(Self::Int16(number.parse().map_err(parse_err)?)),
                    ValueType::Int32 => Ok(Self::Int32(number.parse().map_err(parse_err)?)),
                    ValueType::Int64 => Ok(Self::Int64(number.parse().map_err(parse_err)?)),
                    ValueType::UInt8 => Ok(Self::UInt8(number.parse().map_err(parse_err)?)),
                    ValueType::UInt16 => Ok(Self::UInt16(number.parse().map_err(parse_err)?)),
                    ValueType::UInt32 => Ok(Self::UInt32(number.parse().map_err(parse_err)?)),
                    ValueType::UInt64 => Ok(Self::UInt64(number.parse().map_err(parse_err)?)),
                    _ => Err(Error::UnsupportedColumnType(format!(
                        "Cannot convert number to {value_type:?}",
                    ))),
                }
            }
            SQLValue::Boolean(bool_value) => {
                if value_type != &ValueType::Bool {
                    return Err(Error::InvalidSource);
                }
                Ok(Self::Bool(bool_value))
            }
            SQLValue::Placeholder(_) => Err(Error::UnsupportedColumnType(
                "Plan to add placeholder support".to_string(),
            )),
            column_type => Err(Error::UnsupportedColumnType(column_type.to_string())),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub enum ValueType {
    Null,
    String,
    Uuid,
    Bool,

    Int8,
    Int16,
    Int32,
    Int64,

    UInt8,
    UInt16,
    UInt32,
    UInt64,
}

impl TryFrom<&SQLDatatype> for ValueType {
    type Error = Error;

    fn try_from(value: &SQLDatatype) -> Result<Self> {
        match value {
            SQLDatatype::String(_) => Ok(Self::String),
            SQLDatatype::Uuid => Ok(Self::Uuid),
            SQLDatatype::Bool => Ok(Self::Bool),
            SQLDatatype::Int8(_) => Ok(Self::Int8),
            SQLDatatype::Int16 => Ok(Self::Int16),
            SQLDatatype::Int32 => Ok(Self::Int32),
            SQLDatatype::Int64 => Ok(Self::Int64),
            SQLDatatype::UInt8 => Ok(Self::UInt8),
            SQLDatatype::UInt16 => Ok(Self::UInt16),
            SQLDatatype::UInt32 => Ok(Self::UInt32),
            SQLDatatype::UInt64 => Ok(Self::UInt64),
            column_type => Err(Error::UnsupportedColumnType(column_type.to_string())),
        }
    }
}

impl Value {
    pub fn get_type(&self) -> ValueType {
        match &self {
            Value::Null => ValueType::Null,
            Value::String(_) => ValueType::String,
            Value::Uuid(_) => ValueType::Uuid,
            Value::Bool(_) => ValueType::Bool,
            Value::Int8(_) => ValueType::Int8,
            Value::Int16(_) => ValueType::Int16,
            Value::Int32(_) => ValueType::Int32,
            Value::Int64(_) => ValueType::Int64,
            Value::UInt8(_) => ValueType::UInt8,
            Value::UInt16(_) => ValueType::UInt16,
            Value::UInt32(_) => ValueType::UInt32,
            Value::UInt64(_) => ValueType::UInt64,
        }
    }
}
/// Compares two values and returns their ordering.
pub fn compare_values(left: &Value, right: &Value) -> Result<Ordering> {
    match (left, right) {
        (Value::String(l), Value::String(r)) => Ok(l.cmp(r)),
        (Value::Bool(l), Value::Bool(r)) => Ok(l.cmp(r)),
        (Value::Uuid(l), Value::Uuid(r)) => Ok(l.cmp(r)),

        (
            l @ (Value::Int8(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_)),
            r @ (Value::Int8(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_)),
        ) => Ok(to_i64(l).cmp(&to_i64(r))),

        (
            l @ (Value::UInt8(_) | Value::UInt16(_) | Value::UInt32(_) | Value::UInt64(_)),
            r @ (Value::UInt8(_) | Value::UInt16(_) | Value::UInt32(_) | Value::UInt64(_)),
        ) => Ok(to_u64(l).cmp(&to_u64(r))),

        (
            l @ (Value::Int8(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_)),
            r @ (Value::UInt8(_) | Value::UInt16(_) | Value::UInt32(_) | Value::UInt64(_)),
        ) => compare_signed_unsigned(to_i64(l), to_u64(r)),

        (
            l @ (Value::UInt8(_) | Value::UInt16(_) | Value::UInt32(_) | Value::UInt64(_)),
            r @ (Value::Int8(_) | Value::Int16(_) | Value::Int32(_) | Value::Int64(_)),
        ) => compare_signed_unsigned(to_i64(r), to_u64(l)).map(|o| o.reverse()),

        _ => Err(Error::UnsupportedCommand(
            "Type mismatch in comparison".to_string(),
        )),
    }
}

fn to_i64(val: &Value) -> i64 {
    match val {
        Value::Int8(v) => *v as i64,
        Value::Int16(v) => *v as i64,
        Value::Int32(v) => *v as i64,
        Value::Int64(v) => *v,
        _ => unreachable!(),
    }
}

fn to_u64(val: &Value) -> u64 {
    match val {
        Value::UInt8(v) => *v as u64,
        Value::UInt16(v) => *v as u64,
        Value::UInt32(v) => *v as u64,
        Value::UInt64(v) => *v,
        _ => unreachable!(),
    }
}

fn compare_signed_unsigned(signed: i64, unsigned: u64) -> Result<Ordering> {
    if signed < 0 || unsigned > i64::MAX as u64 {
        Ok(Ordering::Less)
    } else {
        Ok(signed.cmp(&(unsigned as i64)))
    }
}
