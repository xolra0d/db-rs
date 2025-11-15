use std::io::{Read as _, Write as _};

use crate::error::{Error, Result};
use crate::storage::{CompressionType, ValueType};

impl ValueType {
    pub fn get_optimal_compression(&self) -> CompressionType {
        CompressionType::LZ4(3)
    }
}

pub fn compress_bytes(bytes: &[u8], compression_type: CompressionType) -> Result<Vec<u8>> {
    match compression_type {
        CompressionType::LZ4(level) => {
            let output = Vec::new();
            let mut encoder = lz4::EncoderBuilder::new()
                .level(u32::from(level))
                .build(output)
                .map_err(|_| Error::CouldNotInsertData("Could not compress data.".to_string()))?;
            encoder
                .write_all(bytes)
                .map_err(|_| Error::CouldNotInsertData("Could not compress data.".to_string()))?;
            let (output, _compression) = encoder.finish();
            Ok(output)
        }
        CompressionType::None => Ok(bytes.to_vec()),
    }
}

pub fn decompress_bytes(
    compressed_bytes: &[u8],
    compression_type: &CompressionType,
) -> Result<Vec<u8>> {
    match compression_type {
        CompressionType::LZ4(_) => {
            let mut decoder = lz4::Decoder::new(compressed_bytes).map_err(|e| {
                Error::CouldNotReadData(format!("Failed to create LZ4 decoder: {e}"))
            })?;
            let mut decompressed = Vec::new();
            decoder.read_to_end(&mut decompressed).map_err(|e| {
                Error::CouldNotReadData(format!("Failed to decompress LZ4 data: {e}",))
            })?;
            Ok(decompressed)
        }
        CompressionType::None => Ok(compressed_bytes.to_vec()),
    }
}
