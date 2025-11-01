use crate::error::Error;
use serde::{Deserialize, Serialize};
use sqlparser::ast::{DataType as SQLDatatype, Value as SQLValue};
use uuid::Uuid;

/// Represents a parsed value in our custom protocol
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum Value {
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

impl Value {
    pub const fn get_type(&self) -> ValueType {
        match self {
            Self::String(_) => ValueType::String,
            Self::Uuid(_) => ValueType::Uuid,
            Self::Bool(_) => ValueType::Bool,
            Self::Int8(_) => ValueType::Int8,
            Self::Int16(_) => ValueType::Int16,
            Self::Int32(_) => ValueType::Int32,
            Self::Int64(_) => ValueType::Int64,
            Self::UInt8(_) => ValueType::UInt8,
            Self::UInt16(_) => ValueType::UInt16,
            Self::UInt32(_) => ValueType::UInt32,
            Self::UInt64(_) => ValueType::UInt64,
        }
    }
}

impl TryFrom<(SQLValue, ValueType)> for Value {
    type Error = Error;
    fn try_from(value: (SQLValue, ValueType)) -> Result<Self, Self::Error> {
        let (sql_value, value_type) = value;

        match sql_value {
            SQLValue::SingleQuotedString(string)
            | SQLValue::TripleSingleQuotedString(string)
            | SQLValue::TripleDoubleQuotedString(string) => {
                if value_type != ValueType::String {
                    return Err(Error::InvalidSource);
                }
                Ok(Self::String(string))
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
                if value_type != ValueType::Bool {
                    return Err(Error::InvalidSource);
                }
                Ok(Self::Bool(bool_value))
            }
            SQLValue::Null | SQLValue::Placeholder(_) => Err(Error::UnsupportedColumnType(
                "Plan to add Null and placeholder support".to_string(),
            )),
            column_type => Err(Error::UnsupportedColumnType(column_type.to_string())),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone, Hash, PartialEq, Eq)]
pub enum ValueType {
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

    fn try_from(value: &SQLDatatype) -> Result<Self, Self::Error> {
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
