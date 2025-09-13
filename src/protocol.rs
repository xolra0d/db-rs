// We expect for client to send 100% valid sequence of bytes.
// If it fails to do so, it's its fault for breaking the server.
// Enjoy ;)

use std::convert::TryFrom;
use tokio_util::{
    bytes::{Bytes, BytesMut},
    codec::Decoder,
};

const HEADER_SIZE: usize = 2;

#[derive(PartialEq, Clone, Debug)]
pub enum Command {
    String(Bytes),
    Array(Vec<Command>),
}

#[derive(Debug)]
#[allow(dead_code)]
pub enum CommandError {
    UnknownCommand,
    IOError(std::io::Error),
}

impl From<std::io::Error> for CommandError {
    fn from(e: std::io::Error) -> CommandError {
        CommandError::IOError(e)
    }
}

type CommandResult<T> = Result<T, CommandError>;

impl Command {
    fn parse_unknown(buf: &Bytes, start_pos: usize) -> CommandResult<(usize, Command)> {
        match buf[start_pos] {
            b'*' => Self::parse_array(buf, start_pos + 1),
            b'-' => Self::parse_string(buf, start_pos + 1),
            _ => Err(CommandError::UnknownCommand),
        }
    }

    fn parse_simple(buf: &Bytes, start_pos: usize) -> (usize, Bytes) {
        let char_count = buf[start_pos] as usize;
        (
            start_pos + 1 + char_count,
            buf.slice(start_pos + 1..start_pos + 1 + char_count),
        )
    }

    fn parse_string(buf: &Bytes, start_pos: usize) -> CommandResult<(usize, Command)> {
        let (start_pos_next, bytes) = Self::parse_simple(buf, start_pos);
        Ok((start_pos_next, Command::String(bytes)))
    }

    fn parse_array(buf: &Bytes, start_pos: usize) -> CommandResult<(usize, Command)> {
        let element_count = buf[start_pos];
        let mut pos = start_pos + 1;
        let mut result = Vec::new();

        for _ in 0..element_count {
            let (start_pos_next, command) = Self::parse_unknown(buf, pos)?;
            pos = start_pos_next;
            result.push(command);
        }

        Ok((pos, Command::Array(result)))
    }
}

impl TryFrom<Bytes> for Command {
    type Error = CommandError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::parse_unknown(&value, 0).map(|(_, command)| command)
    }
}

pub struct Protocol;

impl Decoder for Protocol {
    type Item = Command;
    type Error = CommandError;

    fn decode(&mut self, buf: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        if buf.len() < HEADER_SIZE {
            return Ok(None);
        }
        let body_size = u16::from_be_bytes([buf[0], buf[1]]) as usize;
        if buf.len() < HEADER_SIZE + body_size {
            buf.reserve(body_size);
            return Ok(None);
        }

        let our_data = buf.split_to(HEADER_SIZE + body_size).freeze();
        let our_data = our_data.slice(HEADER_SIZE..);
        println!("bytes left: {:?}", buf);
        Command::try_from(our_data).map(Some)
    }
}

#[cfg(test)]
mod tests {
    use super::Command::*;
    use super::*;

    #[test]
    fn test_simple() {
        let buf = Bytes::from(&[4, b'E', b'C', b'H', b'O'] as &[u8]);
        assert_eq!(
            Command::parse_simple(&buf, 0),
            (5, Bytes::from_static(b"ECHO"))
        );
    }

    #[test]
    fn test_string() {
        let buf = Bytes::from(&[b'-', 4, b'E', b'C', b'H', b'O'] as &[u8]);
        assert_eq!(
            Command::parse_unknown(&buf, 0).unwrap(),
            (6, String(Bytes::from_static(b"ECHO")))
        );
    }

    #[test]
    fn test_string_array() {
        let buf = Bytes::from(&[
            b'*', 2, b'-', 4, b'E', b'C', b'H', b'O', b'-', 2, b'O', b'K',
        ] as &[u8]);

        assert_eq!(
            Command::parse_unknown(&buf, 0).unwrap(),
            (
                12,
                Array(vec![
                    String(Bytes::from_static(b"ECHO")),
                    String(Bytes::from_static(b"OK"))
                ])
            )
        );
    }
}
