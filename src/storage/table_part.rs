use log::{info, warn};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use uuid::Uuid;

use crate::engines;
use crate::engines::EngineConfig;
use crate::error::{Error, Result};
use crate::runtime_config::{TABLE_DATA, TableConfig};
use crate::storage::compression::{compress_bytes, decompress_bytes};
use crate::storage::table_metadata::{
    TABLE_METADATA_FILENAME, TABLE_METADATA_MAGIC_BYTES, TableMetadata,
};
use crate::storage::{Column, ColumnDef, TableDef, Value, read_file_with_crc, write_file_with_crc};

pub const MAGIC_BYTES_COLUMN: &[u8] = b"THDATA".as_slice();
pub const MAGIC_BYTES_INFO: &[u8] = b"THINDX".as_slice();
pub const PART_INFO_FILENAME: &str = "part.inf";

/// Represents a start byte position and end byte position of the
/// compressed granule.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MarkInfo {
    start: u64,
    end: u64,
}

/// Represents a first row of each granule as well as it's starting position and ending.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Mark {
    pub index: Vec<Value>,
    pub info: Vec<MarkInfo>, // compression
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TablePartInfo {
    pub name: String,
    pub row_count: u64, // max rows per tablepart = 18_446_744_073_709_551_615
    pub marks: Vec<Mark>,
    pub column_defs: Vec<ColumnDef>,
}

impl TablePartInfo {
    pub fn get_column_path(&self, table_def: &TableDef, column_def: &ColumnDef) -> PathBuf {
        table_def
            .get_path()
            .join(&self.name)
            .join(format!("{}.bin", column_def.name))
    }

    /// Reads and decompresses a column from disk using granule-based storage.
    ///
    /// Reads specified granules according to MarkInfo slice, decompresses them, and combines into a Column.
    ///
    /// Args:
    ///   * table_def: Table definition for path resolution
    ///   * column_def: Column to read
    ///   * mark_infos: Slice of MarkInfo for granules to read (selective reading)
    ///
    /// Returns: Column with data from specified granules or CouldNotReadData on failure
    pub fn read_column(
        &self,
        table_def: &TableDef,
        column_def: &ColumnDef,
        mark_infos: &[MarkInfo],
    ) -> Result<Column> {
        let column_path = self.get_column_path(table_def, column_def);

        let file_bytes = std::fs::read(&column_path)
            .map_err(|e| Error::CouldNotReadData(format!("Failed to read column file: {}", e)))?;

        if file_bytes.len() <= MAGIC_BYTES_COLUMN.len() + 4 {
            return Err(Error::CouldNotReadData("Column file too small".to_string()));
        }

        let file_magic_bytes = &file_bytes[0..MAGIC_BYTES_COLUMN.len()];
        if file_magic_bytes != MAGIC_BYTES_COLUMN {
            return Err(Error::CouldNotReadData(
                "Invalid magic bytes in column file".to_string(),
            ));
        }

        // Verify CRC
        let data_bytes = &file_bytes[MAGIC_BYTES_COLUMN.len()..(file_bytes.len() - 4)];
        let expected_crc = u32::from_le_bytes([
            file_bytes[file_bytes.len() - 4],
            file_bytes[file_bytes.len() - 3],
            file_bytes[file_bytes.len() - 2],
            file_bytes[file_bytes.len() - 1],
        ]);
        let actual_crc = crc32fast::hash(data_bytes);
        if expected_crc != actual_crc {
            return Err(Error::CouldNotReadData(
                "CRC mismatch in column file".to_string(),
            ));
        }

        let mut all_values = Vec::new();

        for mark_info in mark_infos {
            // Adjusting start,end to exclude magic bytes
            let start = (mark_info.start as usize) - MAGIC_BYTES_COLUMN.len();
            let end = (mark_info.end as usize) - MAGIC_BYTES_COLUMN.len();
            let granule_compressed_bytes = &data_bytes[start..end];

            let granule_bytes =
                decompress_bytes(granule_compressed_bytes, column_def.field_type.clone())?;

            let granule_values: Vec<Value> =
                bincode::serde::decode_from_slice(&granule_bytes, bincode::config::standard())
                    .map(|x| x.0)
                    .map_err(|e| {
                        Error::CouldNotReadData(format!("Failed to deserialize granule: {}", e))
                    })?;

            all_values.extend(granule_values);
        }

        Ok(Column {
            column_def: column_def.clone(),
            data: all_values,
        })
    }
}

/// Immutable table part information.
#[derive(Debug, Clone)]
pub struct TablePart {
    pub info: TablePartInfo,
    pub data: Vec<Column>,
}

impl TablePart {
    /// Creates a new table part with generated UUID name and indexes.
    ///
    /// Orders columns according to engine requirements and generates primary indexes
    /// for ORDER BY columns with INDEX_GRANULARITY.
    ///
    /// Returns: (TablePart, ordered columns) or engine error
    pub fn try_new(table_metadata: &TableMetadata, columns: Vec<Column>) -> Result<Self> {
        if columns.is_empty() || columns[0].data.is_empty() {
            return Err(Error::InvalidSource);
        }
        let name = Uuid::now_v7().to_string();

        let engine = engines::get_engine(&table_metadata.settings.engine, EngineConfig::default());
        let data = engine.order_columns(columns, &table_metadata.schema.order_by)?;

        let marks = generate_indexes(
            &data,
            &table_metadata.schema.order_by,
            table_metadata.settings.index_granularity,
        );
        let row_count = data[0].data.len() as u64;

        let info = TablePartInfo {
            name,
            marks,
            row_count,
            column_defs: data.iter().map(|col| col.column_def.clone()).collect(),
        };

        Ok(Self { info, data })
    }

    /// Saves part data and indexes to raw directory.
    ///
    /// Writes each column to separate .bin file and info to `PART_INFO_FILENAME`.
    /// All files include magic bytes and CRC32 checksums.
    ///
    /// Returns: Ok or CouldNotInsertData on I/O failure
    pub fn save_raw(&mut self, table_def: &TableDef, index_granularity: u32) -> Result<()> {
        let raw_dir = self.get_raw_dir(table_def);
        std::fs::create_dir_all(&raw_dir)
            .map_err(|_| Error::CouldNotInsertData("Failed to create raw directory".to_string()))?;

        for col_idx in 0..self.data.len() {
            let column_file = raw_dir.join(format!("{}.bin", self.data[col_idx].column_def.name));
            self.write_column_with_marks(col_idx, &column_file, index_granularity)?;
        }

        let info_file = raw_dir.join(PART_INFO_FILENAME);
        write_file_with_crc(&self.info, &info_file, MAGIC_BYTES_INFO)?;

        Ok(())
    }

    /// Writes a single column file with granule-by-granule serialization and populates MarkInfo.
    fn write_column_with_marks(
        &mut self,
        col_idx: usize,
        path: &PathBuf,
        index_granularity: u32,
    ) -> Result<()> {
        let mut file_bytes = Vec::from(MAGIC_BYTES_COLUMN);
        let granule_size = index_granularity as usize;
        let total_rows = self.data[col_idx].data.len();

        for (granule_idx, chunk_start) in (0..total_rows).step_by(granule_size).enumerate() {
            let chunk_end = (chunk_start + granule_size).min(total_rows);
            let granule_data = &self.data[col_idx].data[chunk_start..chunk_end];

            let start_pos = file_bytes.len() as u64;

            let granule_bytes =
                bincode::serde::encode_to_vec(granule_data, bincode::config::standard()).map_err(
                    |e| Error::CouldNotInsertData(format!("Failed to serialize granule: {}", e)),
                )?;
            let granule_bytes = compress_bytes(&granule_bytes, granule_data[0].get_type())?;
            file_bytes.extend(&granule_bytes);
            let end_pos = file_bytes.len() as u64;

            if granule_idx >= self.info.marks.len() {
                return Err(Error::CouldNotInsertData(
                    "Invalid number of granules. Most probably different column sizes".to_string(),
                ));
            }

            self.info.marks[granule_idx].info.push(MarkInfo {
                start: start_pos,
                end: end_pos,
            });
        }

        let data_bytes = &file_bytes[MAGIC_BYTES_COLUMN.len()..];
        let crc = crc32fast::hash(data_bytes);
        file_bytes.extend(crc.to_le_bytes());

        std::fs::write(path, file_bytes)
            .map_err(|e| Error::CouldNotInsertData(format!("Failed to write file: {}", e)))
    }

    /// Atomically moves part from raw to normal directory and updates in-memory index.
    ///
    /// Updates memory first (under exclusive lock), then renames directory.
    /// Rolls back memory change on filesystem failure.
    ///
    /// Returns: Ok or CouldNotInsertData with rollback on failure
    pub fn move_to_normal(&self, table_def: &TableDef) -> Result<()> {
        let raw_dir = self.get_raw_dir(table_def);
        let normal_dir = table_def.get_path().join(&self.info.name);

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
        let index;
        unsafe {
            entry.get_mut().infos.push(self.info.clone());
            index = entry.infos.len() - 1;
        }

        // atomic move
        if let Err(e) = std::fs::rename(&raw_dir, &normal_dir) {
            // Rollback: remove the part we just added
            unsafe {
                entry.get_mut().infos.remove(index);
            }
            return Err(Error::CouldNotInsertData(format!(
                "Failed to move part directory: {e}"
            )));
        }

        Ok(())
    }

    fn get_raw_dir(&self, table_def: &TableDef) -> PathBuf {
        table_def.get_path().join("raw").join(&self.info.name)
    }
}

fn generate_indexes(
    columns: &[Column],
    order_by: &[ColumnDef],
    index_granularity: u32,
) -> Vec<Mark> {
    let columns_in_order_by: Vec<&Column> = columns
        .iter()
        .filter(|x| order_by.contains(&x.column_def))
        .collect();

    let total_rows = columns_in_order_by.first().map_or(0, |x| x.data.len());
    let num_granules = total_rows.div_ceil(index_granularity as usize);
    let mut marks = Vec::with_capacity(num_granules);

    for row_idx in (0..total_rows).step_by(index_granularity as usize) {
        let row_values: Vec<Value> = columns_in_order_by
            .iter()
            .map(|x| x.data[row_idx].clone())
            .collect();
        marks.push(Mark {
            index: row_values,
            info: Vec::new(), // Will be filled during `save_raw`
        });
    }
    marks
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

            let Ok(table_metadata) = read_file_with_crc::<TableMetadata>(
                &table_def.get_path().join(TABLE_METADATA_FILENAME),
                TABLE_METADATA_MAGIC_BYTES,
            ) else {
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

                let part_info_file_path = part_path.join(PART_INFO_FILENAME);
                match read_file_with_crc(&part_info_file_path, MAGIC_BYTES_INFO) {
                    Ok(info) => {
                        let mut entry =
                            TABLE_DATA
                                .entry_sync(table_def.clone())
                                .or_insert(TableConfig {
                                    metadata: table_metadata.clone(),
                                    infos: Vec::new(),
                                });

                        // This is safe, because
                        // we receive exclusive access to the entry (as uses `OccupiedEntry`),
                        // and no other thread can modify it.
                        unsafe {
                            entry.get_mut().infos.push(info);
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
