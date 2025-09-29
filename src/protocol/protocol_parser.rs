use crate::protocol::command::{Command, CommandError, CommandResult};

use tokio_util::bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

const BYTE_SIZE: usize = 8;
const HEADER_SIZE: usize = u32::BITS as usize / BYTE_SIZE;

#[derive(Debug)]
pub enum SendError {
    IOError(std::io::Error),
}

impl From<std::io::Error> for SendError {
    fn from(e: std::io::Error) -> SendError {
        SendError::IOError(e)
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
        let our_data = buf.split_to(header).freeze();
        Command::try_from(our_data).map(Some)
    }
}

impl Encoder<CommandResult<Command>> for Protocol {
    type Error = SendError;

    fn encode(
        &mut self,
        item: CommandResult<Command>,
        buf: &mut BytesMut,
    ) -> Result<(), Self::Error> {
        match item {
            Ok(item) => {
                let command_bytes = item.encode_command();
                let message_size = command_bytes.len() as u32;
                buf.put_u32(message_size); // HEADER
                buf.put(item.encode_command().as_slice()); // BODY
            }
            Err(error) => {
                let error_bytes = error.encode_error();
                let message_size = error_bytes.len() as u32;
                buf.put_u32(message_size); // HEADER
                buf.put(error_bytes.as_slice()); // BODY
            }
        };
        Ok(())
    }
}
