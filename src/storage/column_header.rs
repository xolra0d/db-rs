use crate::engine::FieldType;
use std::io::{Read, Write};

const MAGIC_BYTES: &[u8; 6] = b"THCOLU";
const VERSION: u16 = 1;
pub const HEADER_SIZE: usize = 16;

#[derive(Debug, Clone, PartialEq)]
pub struct ColumnHeader {
    pub version: u16,
    pub field_type: FieldType,
    pub flags: u8,
}

impl ColumnHeader {
    pub fn new(field_type: FieldType) -> Self {
        Self {
            version: VERSION,
            field_type,
            flags: 0,
        }
    }

    pub fn write(&self, writer: &mut impl Write) -> std::io::Result<()> {
        let type_byte: u8 = match self.field_type {
            FieldType::String => 1,
            FieldType::Array => 2,
        };

        let mut buffer = Vec::with_capacity(HEADER_SIZE);

        buffer.extend_from_slice(MAGIC_BYTES);
        buffer.extend_from_slice(&self.version.to_le_bytes());
        buffer.push(type_byte);
        buffer.push(self.flags);
        buffer.extend_from_slice(&[0u8; 6]); // reserved for future use

        writer.write_all(&buffer)?;

        Ok(())
    }

    #[allow(dead_code)] // TODO: remove later
    pub fn read(reader: &mut impl Read) -> std::io::Result<Self> {
        let mut magic = [0u8; 6];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid column file: wrong magic bytes",
            ));
        }

        let mut version_bytes = [0u8; 2];
        reader.read_exact(&mut version_bytes)?;
        let version = u16::from_le_bytes(version_bytes);

        if version > VERSION {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unsupported version: {}", version),
            ));
        }

        let mut type_byte = [0u8; 1];
        reader.read_exact(&mut type_byte)?;
        let field_type = match type_byte[0] {
            1 => FieldType::String,
            2 => FieldType::Array,
            _ => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "Invalid field type",
                ));
            }
        };

        let mut flags = [0u8; 1];
        reader.read_exact(&mut flags)?;

        let mut reserved = [0u8; 6];
        reader.read_exact(&mut reserved)?;

        Ok(Self {
            version,
            field_type,
            flags: flags[0],
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_column_header_write_read() {
        let header = ColumnHeader::new(FieldType::String);
        let mut buffer = Vec::new();

        header.write(&mut buffer).unwrap();
        assert_eq!(buffer.len(), HEADER_SIZE);

        let mut cursor = Cursor::new(buffer);
        let read_header = ColumnHeader::read(&mut cursor).unwrap();

        assert_eq!(read_header, header);
    }

    #[test]
    fn test_column_header_invalid_magic() {
        let mut buffer = vec![0u8; HEADER_SIZE];
        buffer[0..6].copy_from_slice(b"WRONG\0");

        let mut cursor = Cursor::new(buffer);
        let result = ColumnHeader::read(&mut cursor);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong magic bytes")
        );
    }
}
