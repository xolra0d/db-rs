use crate::engine::FieldType;
use serde::{Deserialize, Serialize};
use std::io::{Read, Seek, SeekFrom, Write};
use std::time::{SystemTime, UNIX_EPOCH};

const MAGIC_BYTES: &[u8; 6] = b"THMETA";
const VERSION: u16 = 1;
pub const _HEADER_SIZE: usize = 32;

pub mod flags {
    pub const NONE: u32 = 0x00000000;
    pub const _COMPRESSED: u32 = 0x00000001;
    pub const _HAS_WAL: u32 = 0x00000100;
    pub const _STRICT_SCHEMA: u32 = 0x00000200;
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnDef {
    pub name: String,
    pub field_type: FieldType,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct TableSchema {
    pub columns: Vec<ColumnDef>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableMetadata {
    pub version: u16,
    pub schema_version: u32,
    pub flags: u32,
    pub row_count: u64,
    pub created_at: u64,
    pub schema: TableSchema,
}

impl TableMetadata {
    pub fn new(columns: Vec<(String, FieldType)>) -> Self {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();

        Self {
            version: VERSION,
            schema_version: 1,
            flags: flags::NONE,
            row_count: 0,
            created_at: now,
            schema: TableSchema {
                columns: columns
                    .into_iter()
                    .map(|(name, field_type)| ColumnDef { name, field_type })
                    .collect(),
            },
        }
    }

    pub fn write(&self, writer: &mut impl Write) -> std::io::Result<()> {
        let schema_bytes = rmp_serde::to_vec(&self.schema).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to serialize schema: {}", e),
            )
        })?;

        let mut hasher = crc32fast::Hasher::new();
        hasher.update(MAGIC_BYTES);
        hasher.update(&self.version.to_le_bytes());
        hasher.update(&self.schema_version.to_le_bytes());
        hasher.update(&self.flags.to_le_bytes());
        hasher.update(&self.row_count.to_le_bytes());
        hasher.update(&self.created_at.to_le_bytes());
        hasher.update(&schema_bytes);
        let checksum = hasher.finalize();

        let total_size = MAGIC_BYTES.len() + 2 + 4 + 4 + 8 + 8 + schema_bytes.len() + 4;
        let mut buffer = Vec::with_capacity(total_size);

        buffer.extend_from_slice(MAGIC_BYTES);
        buffer.extend_from_slice(&self.version.to_le_bytes());
        buffer.extend_from_slice(&self.schema_version.to_le_bytes());
        buffer.extend_from_slice(&self.flags.to_le_bytes());
        buffer.extend_from_slice(&self.row_count.to_le_bytes());
        buffer.extend_from_slice(&self.created_at.to_le_bytes());
        buffer.extend_from_slice(&schema_bytes);
        buffer.extend_from_slice(&checksum.to_le_bytes());

        writer.write_all(&buffer)?;

        Ok(())
    }

    pub fn read(reader: &mut impl Read) -> std::io::Result<Self> {
        let mut magic = [0u8; 6];
        reader.read_exact(&mut magic)?;
        if &magic != MAGIC_BYTES {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Invalid metadata file: wrong magic bytes",
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

        let mut schema_version_bytes = [0u8; 4];
        reader.read_exact(&mut schema_version_bytes)?;
        let schema_version = u32::from_le_bytes(schema_version_bytes);

        let mut flags_bytes = [0u8; 4];
        reader.read_exact(&mut flags_bytes)?;
        let flags = u32::from_le_bytes(flags_bytes);

        let mut row_count_bytes = [0u8; 8];
        reader.read_exact(&mut row_count_bytes)?;
        let row_count = u64::from_le_bytes(row_count_bytes);

        let mut created_at_bytes = [0u8; 8];
        reader.read_exact(&mut created_at_bytes)?;
        let created_at = u64::from_le_bytes(created_at_bytes);

        // Read remaining bytes (schema + checksum)
        let mut remaining = Vec::new();
        reader.read_to_end(&mut remaining)?;

        if remaining.len() < 4 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Missing checksum",
            ));
        }

        // Split schema and checksum
        let schema_bytes = &remaining[..remaining.len() - 4];
        let stored_checksum = u32::from_le_bytes([
            remaining[remaining.len() - 4],
            remaining[remaining.len() - 3],
            remaining[remaining.len() - 2],
            remaining[remaining.len() - 1],
        ]);

        // Verify checksum
        let mut hasher = crc32fast::Hasher::new();
        hasher.update(&magic);
        hasher.update(&version_bytes);
        hasher.update(&schema_version_bytes);
        hasher.update(&flags_bytes);
        hasher.update(&row_count_bytes);
        hasher.update(&created_at_bytes);
        hasher.update(schema_bytes);
        let calculated_checksum = hasher.finalize();

        if calculated_checksum != stored_checksum {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Checksum mismatch: metadata file corrupted",
            ));
        }

        // Deserialize schema
        let schema: TableSchema = rmp_serde::from_slice(schema_bytes).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Failed to deserialize schema: {}", e),
            )
        })?;

        Ok(Self {
            version,
            schema_version,
            flags,
            row_count,
            created_at,
            schema,
        })
    }

    /// Update row count in an existing file
    pub fn update_row_count(
        file: &mut (impl Seek + Read + Write),
        new_count: u64,
    ) -> std::io::Result<()> {
        // Read the current metadata
        file.seek(SeekFrom::Start(0))?;
        let mut metadata = Self::read(file)?;

        // Update the row count
        metadata.row_count = new_count;

        // Write back the entire metadata (with updated checksum)
        file.seek(SeekFrom::Start(0))?;
        metadata.write(file)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_table_metadata_write_read() {
        let columns = vec![
            ("name".to_string(), FieldType::String),
            ("tags".to_string(), FieldType::Array),
        ];
        let metadata = TableMetadata::new(columns);

        let mut buffer = Vec::new();
        metadata.write(&mut buffer).unwrap();

        let mut cursor = Cursor::new(buffer);
        let read_metadata = TableMetadata::read(&mut cursor).unwrap();

        assert_eq!(read_metadata.version, metadata.version);
        assert_eq!(read_metadata.schema_version, metadata.schema_version);
        assert_eq!(read_metadata.flags, metadata.flags);
        assert_eq!(read_metadata.row_count, metadata.row_count);
        assert_eq!(read_metadata.created_at, metadata.created_at);
        assert_eq!(read_metadata.schema, metadata.schema);
    }

    #[test]
    fn test_table_metadata_update_row_count() {
        let columns = vec![
            ("name".to_string(), FieldType::String),
            ("tags".to_string(), FieldType::Array),
        ];
        let metadata = TableMetadata::new(columns);

        let mut buffer = Vec::new();
        metadata.write(&mut buffer).unwrap();

        let mut cursor = Cursor::new(buffer);
        let read_metadata = TableMetadata::read(&mut cursor).unwrap();

        assert_eq!(read_metadata.version, metadata.version);
        assert_eq!(read_metadata.schema_version, metadata.schema_version);
        assert_eq!(read_metadata.flags, metadata.flags);
        assert_eq!(read_metadata.row_count, metadata.row_count);
        assert_eq!(read_metadata.created_at, metadata.created_at);
        assert_eq!(read_metadata.schema, metadata.schema);
    }

    #[test]
    fn test_table_metadata_invalid_magic() {
        let mut buffer = vec![0u8; 50];
        buffer[0..6].copy_from_slice(b"WRONGM");

        let mut cursor = Cursor::new(buffer);
        let result = TableMetadata::read(&mut cursor);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("wrong magic bytes")
        );
    }

    #[test]
    fn test_table_metadata_checksum_validation() {
        let columns = vec![("name".to_string(), FieldType::String)];
        let metadata = TableMetadata::new(columns);

        let mut buffer = Vec::new();
        metadata.write(&mut buffer).unwrap();

        let len = buffer.len();
        buffer[len - 1] ^= 0xFF; // Corrupt the checksum

        let mut cursor = Cursor::new(buffer);
        let result = TableMetadata::read(&mut cursor);

        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Checksum mismatch")
        );
    }
}
