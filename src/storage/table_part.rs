use log::{info, warn};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::engines;
use crate::error::{Error, Result};
use crate::runtime_config::{TABLE_DATA, TableConfig};
use crate::storage::table_metadata::TableMetadata;
use crate::storage::{Column, ColumnDef, TableDef};

const MAGIC_BYTES_DATA: &[u8] = b"THDATA".as_slice();
const MAGIC_BYTES_INDEX: &[u8] = b"THINDX".as_slice();

/// Immutable table part used to store insert data.
#[derive(Debug, Clone)]
pub struct TablePart {
    pub name: String,
    pub indexes: Vec<Column>,
}

impl TablePart {
    /// Creates a new table part with generated UUID name and indexes.
    ///
    /// Orders columns according to engine requirements and generates primary indexes
    /// for ORDER BY columns with INDEX_GRANULARITY.
    ///
    /// Returns: (TablePart, ordered columns) or engine error
    pub fn try_new(
        table_metadata: &TableMetadata,
        columns: Vec<Column>,
    ) -> Result<(Self, Vec<Column>)> {
        let name = Uuid::now_v7().to_string();

        let engine = engines::get_engine(&table_metadata.settings.engine, None);
        let columns = engine.order_columns(columns, &table_metadata.schema.order_by)?;

        let indexes = generate_indexes(
            &columns,
            &table_metadata.schema.order_by,
            table_metadata.settings.index_granularity,
        );

        Ok((Self { name, indexes }, columns))
    }

    /// Saves part data and indexes to raw directory.
    ///
    /// Writes each column to separate .bin file and indexes to primary.idx.
    /// All files include magic bytes and CRC32 checksums.
    ///
    /// Returns: Ok or CouldNotInsertData on I/O failure
    pub fn save_raw(&self, table_def: &TableDef, data: &[Column]) -> Result<()> {
        let raw_dir = self.get_raw_dir(table_def);
        std::fs::create_dir_all(&raw_dir)
            .map_err(|_| Error::CouldNotInsertData("Failed to create raw directory".to_string()))?;

        for column in data {
            let column_file = raw_dir.join(format!("{}.bin", column.column_def.name));
            Self::write_column_to_file(column, &column_file, MAGIC_BYTES_DATA)?;
        }

        let index_file = raw_dir.join("primary.idx");
        Self::write_columns_to_file(&self.indexes, &index_file, MAGIC_BYTES_INDEX)?;

        Ok(())
    }

    /// Atomically moves part from raw to normal directory and updates in-memory index.
    ///
    /// Updates memory first (under exclusive lock), then renames directory.
    /// Rolls back memory change on filesystem failure.
    ///
    /// Returns: Ok or CouldNotInsertData with rollback on failure
    pub fn move_to_normal(&self, table_def: &TableDef) -> Result<()> {
        let raw_dir = self.get_raw_dir(table_def);
        let normal_dir = self.get_normal_dir(table_def);

        let part = self.clone();

        // Acquire exclusive access to TABLE_DATA entry BEFORE filesystem operations
        let Some(mut entry) = TABLE_DATA.get_sync(table_def) else {
            return Err(Error::CouldNotInsertData(
                "Could not store in memory".to_string(),
            ));
        };

        // This is safe because:
        // - We have exclusive access via OccupiedEntry from get_sync()
        // - No other thread can modify this entry while we hold it
        // - The part is cloned, so no lifetime issues
        unsafe {
            entry.get_mut().indexes.push(part);
        }

        // Then persist to disk
        if let Err(e) = std::fs::rename(&raw_dir, &normal_dir) {
            // Rollback: remove the part we just added
            unsafe {
                entry.get_mut().indexes.pop();
            }
            return Err(Error::CouldNotInsertData(format!(
                "Failed to move part directory: {}",
                e
            )));
        }

        Ok(())
    }

    /// Removes raw directory for cleanup after failures.
    ///
    /// Returns: Ok or CouldNotRemoveBadPart
    pub fn remove_raw(&self, table_def: &TableDef) -> Result<()> {
        let raw_dir = self.get_raw_dir(table_def);
        std::fs::remove_dir_all(&raw_dir)
            .map_err(|_| Error::CouldNotRemoveBadPart(self.name.clone()))
    }

    fn get_raw_dir(&self, table_def: &TableDef) -> PathBuf {
        table_def.get_path().join("raw").join(&self.name)
    }

    fn get_normal_dir(&self, table_def: &TableDef) -> PathBuf {
        table_def.get_path().join(&self.name)
    }

    fn write_column_to_file(column: &Column, path: &PathBuf, magic_bytes: &[u8]) -> Result<()> {
        let mut bytes = Vec::from(magic_bytes);

        let data_bytes = bincode::serde::encode_to_vec(column, bincode::config::standard())
            .map_err(|e| Error::CouldNotInsertData(format!("Failed to serialize column: {}", e)))?;

        let crc = crc32fast::hash(&data_bytes);

        bytes.extend(data_bytes);
        bytes.extend(crc.to_le_bytes());

        std::fs::write(path, bytes)
            .map_err(|e| Error::CouldNotInsertData(format!("Failed to write file: {}", e)))
    }

    fn write_columns_to_file(columns: &[Column], path: &PathBuf, magic_bytes: &[u8]) -> Result<()> {
        let mut bytes = Vec::from(magic_bytes);

        let data_bytes = bincode::serde::encode_to_vec(columns, bincode::config::standard())
            .map_err(|e| {
                Error::CouldNotInsertData(format!("Failed to serialize columns: {}", e))
            })?;

        let crc = crc32fast::hash(&data_bytes);

        bytes.extend(data_bytes);
        bytes.extend(crc.to_le_bytes());

        std::fs::write(path, bytes)
            .map_err(|e| Error::CouldNotInsertData(format!("Failed to write file: {}", e)))
    }
}

fn generate_indexes(
    columns: &[Column],
    order_by: &[ColumnDef],
    index_granularity: u32,
) -> Vec<Column> {
    let columns_in_order_by: Vec<&Column> = columns
        .iter()
        .filter(|x| order_by.contains(&x.column_def))
        .collect();

    let mut indexes: Vec<Column> = columns_in_order_by
        .iter()
        .map(|x| Column {
            column_def: x.column_def.clone(),
            data: Vec::new(),
        })
        .collect();

    for (col_idx, column) in columns_in_order_by.iter().enumerate() {
        // U32 AS USIZE (consider check?)
        for col_value in column.data.iter().step_by(index_granularity as usize) {
            indexes[col_idx].data.push(col_value.clone());
        }
    }

    indexes
}

/// Loads all table parts from filesystem into memory on startup.
///
/// Scans all databases and tables, loads part indexes, and populates TABLE_DATA.
/// Cleans up any leftover raw directories from crashes.
///
/// Returns: Ok or CouldNotInsertData on critical failure
pub fn load_all_parts_on_startup(db_dir: &Path) -> Result<()> {
    info!(
        "Loading parts from database directory: {}",
        db_dir.display()
    );

    if !db_dir.exists() {
        warn!("Database directory does not exist: {}", db_dir.display());
        return Ok(());
    }

    let databases = std::fs::read_dir(db_dir).map_err(|e| {
        Error::CouldNotInsertData(format!("Failed to read database directory: {}", e))
    })?;

    for database_entry in databases {
        let database_entry = database_entry.map_err(|e| {
            Error::CouldNotInsertData(format!("Failed to read database entry: {}", e))
        })?;

        let database_path = database_entry.path();
        if !database_path.is_dir() {
            continue;
        }

        let database_name = database_entry.file_name().to_string_lossy().to_string();

        let tables = std::fs::read_dir(&database_path).map_err(|e| {
            Error::CouldNotInsertData(format!(
                "Failed to read tables in database {}: {}",
                database_name, e
            ))
        })?;

        for table_entry in tables {
            let table_entry = table_entry.map_err(|e| {
                Error::CouldNotInsertData(format!("Failed to read table entry: {}", e))
            })?;

            let table_path = table_entry.path();
            if !table_path.is_dir() {
                continue;
            }

            let table_name = table_entry.file_name().to_string_lossy().to_string();
            let table_def = TableDef {
                database: database_name.clone(),
                table: table_name.clone(),
            };

            let Ok(table_metadata) = TableMetadata::read_from(&table_def) else {
                continue;
            };

            let parts = std::fs::read_dir(&table_path).map_err(|e| {
                Error::CouldNotInsertData(format!(
                    "Failed to read parts in table {}: {}",
                    table_def, e
                ))
            })?;

            for part_entry in parts {
                let part_entry = part_entry.map_err(|e| {
                    Error::CouldNotInsertData(format!("Failed to read part entry: {}", e))
                })?;

                let part_path = part_entry.path();
                let part_name = part_entry.file_name().to_string_lossy().to_string();

                if !part_path.is_dir() || part_name.starts_with('.') {
                    continue;
                }

                if part_name == "raw" {
                    match std::fs::remove_dir_all(&part_path) {
                        Ok(_) => {
                            info!("Removed raw directory for table {}", table_def);
                        }
                        Err(e) => {
                            warn!(
                                "Failed to remove raw directory for table {}: {}",
                                table_def, e
                            );
                        }
                    }
                    continue;
                }

                match load_part_indexes(&part_name, &part_path) {
                    Ok(part) => {
                        let mut entry =
                            TABLE_DATA
                                .entry_sync(table_def.clone())
                                .or_insert(TableConfig {
                                    metadata: table_metadata.clone(),
                                    indexes: Vec::new(),
                                });

                        // This is safe, because
                        // we receive exclusive access to the entry (as uses `OccupiedEntry`),
                        // and no other thread can modify it.
                        unsafe {
                            entry.get_mut().indexes.push(part);
                        }

                        info!("Loaded part {} for table {}", part_name, table_def);
                    }
                    Err(e) => {
                        warn!(
                            "Failed to load part {} for table {}: {:?}",
                            part_name, table_def, e
                        );
                    }
                }
            }
        }
    }

    info!("Finished loading parts");
    Ok(())
}

fn load_part_indexes(part_name: &str, part_path: &Path) -> Result<TablePart> {
    let index_file = part_path.join("primary.idx");

    if !index_file.exists() {
        return Err(Error::CouldNotInsertData(format!(
            "Index file not found for part {}",
            part_name
        )));
    }

    let file_bytes = std::fs::read(&index_file)
        .map_err(|e| Error::CouldNotInsertData(format!("Failed to read index file: {}", e)))?;

    if file_bytes.len() <= MAGIC_BYTES_INDEX.len() + 4 {
        return Err(Error::CouldNotInsertData(format!(
            "Index file too small for part {}",
            part_name
        )));
    }

    let file_magic_bytes = &file_bytes[0..MAGIC_BYTES_INDEX.len()];
    if file_magic_bytes != MAGIC_BYTES_INDEX {
        return Err(Error::CouldNotInsertData(format!(
            "Invalid magic bytes in index file for part {}",
            part_name
        )));
    }

    let data_bytes = &file_bytes[MAGIC_BYTES_INDEX.len()..(file_bytes.len() - 4)];

    let expected_crc = u32::from_le_bytes([
        file_bytes[file_bytes.len() - 4],
        file_bytes[file_bytes.len() - 3],
        file_bytes[file_bytes.len() - 2],
        file_bytes[file_bytes.len() - 1],
    ]);

    let actual_crc = crc32fast::hash(data_bytes);
    if expected_crc != actual_crc {
        return Err(Error::CouldNotInsertData(format!(
            "CRC mismatch in index file for part {}",
            part_name
        )));
    }

    let indexes: Vec<Column> =
        bincode::serde::decode_from_slice(data_bytes, bincode::config::standard())
            .map(|x| x.0)
            .map_err(|e| {
                Error::CouldNotInsertData(format!("Failed to deserialize indexes: {}", e))
            })?;

    Ok(TablePart {
        name: part_name.to_string(),
        indexes,
    })
}
