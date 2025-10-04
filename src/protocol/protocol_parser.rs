use crate::protocol::command::{Command, CommandError, CommandResult};

use log::warn;
use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

const HEADER_SIZE: usize = size_of::<u32>();

#[derive(Debug)]
pub enum SendError {
    IOError(String),
}

impl From<std::io::Error> for SendError {
    fn from(e: std::io::Error) -> Self {
        Self::IOError(e.to_string())
    }
}

/// Structure used to implement `tokio_util::codec::{Decoder, Encoder}` traits.
pub struct Protocol;

impl Decoder for Protocol {
    type Item = Command;
    type Error = CommandError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }

        // Peek at the header without consuming it
        let header = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
        let total_message_size = HEADER_SIZE + header;

        if buf.len() < total_message_size {
            buf.reserve(total_message_size - buf.len());
            return Ok(None);
        }

        // Now consume the header and the data
        buf.advance(HEADER_SIZE);
        match rmp_serde::from_slice(&buf.split_to(header).freeze()) {
            Ok(command) => Ok(Some(command)),
            Err(e) => Err(CommandError::ExecutionError(e.to_string())),
        }
    }
}

impl Encoder<CommandResult<Command>> for Protocol {
    type Error = SendError;

    fn encode(
        &mut self,
        item: CommandResult<Command>,
        buf: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        let command_bytes = rmp_serde::to_vec(&item).map_err(|e| {
            let msg = format!("Failed to encode command. Command: {item:?}, error: {e}",);

            warn!("{}", &msg);
            SendError::IOError(msg)
        })?;

        let message_size = command_bytes.len() as u32; // safe cast, bcs max msg length is u32::MAX bytes = 4.3GB 

        buf.put_u32(message_size); // HEADER
        buf.put(command_bytes.as_slice()); // BODY

        Ok(())
    }
}
