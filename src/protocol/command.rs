use std::convert::TryFrom;
use tokio_util::bytes::Bytes;

/// Represents a parsed command in our custom protocol
#[derive(PartialEq, Clone, Debug)]
pub enum Command {
    String(Bytes),
    Array(Vec<Command>),
}

/// Errors that can occur during  command parsing or execution
#[derive(Debug)]
pub enum CommandError {
    UnknownCommandType(u8),
    InvalidCommandName(Bytes),
    ExecutionError(Bytes),
    IOError(std::io::Error),
}

pub type CommandResult<T> = Result<T, CommandError>;

impl Command {
    pub fn get_array(self) -> Option<Vec<Command>> {
        match self {
            Command::Array(vec) => Some(vec),
            _ => None,
        }
    }

    /// Base function for decoding any command type
    /// # Examples:
    /// ```
    /// use bytes::Bytes;
    /// let buf = Bytes::from(&[4, b'E', b'C', b'H', b'O'] as &[u8]);
    /// assert_eq!(Command::parse_simple(&buf, 0), (5, Bytes::from_static(b"ECHO")));
    /// ```
    fn parse_simple(buf: &Bytes, start_pos: usize) -> (usize, Bytes) {
        let char_count = buf[start_pos] as usize;
        (
            start_pos + 1 + char_count,
            buf.slice(start_pos + 1..start_pos + 1 + char_count),
        )
    }

    /// Puts bytes array into Command::String variant
    // TODO: add bytes validation
    fn parse_string(buf: &Bytes, start_pos: usize) -> CommandResult<(usize, Command)> {
        let (start_pos_next, bytes) = Self::parse_simple(buf, start_pos);
        Ok((start_pos_next, Command::String(bytes)))
    }

    /// Decodes an array of items
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

    /// "Start" function for parsing not yet known type AT `start_pos` byte
    fn parse_unknown(buf: &Bytes, start_pos: usize) -> CommandResult<(usize, Command)> {
        match buf[start_pos] {
            b'*' => Self::parse_array(buf, start_pos + 1),
            b'-' => Self::parse_string(buf, start_pos + 1),
            _ => Err(CommandError::UnknownCommandType(buf[start_pos])),
        }
    }

    /// Encode Command variant through protocol idea
    pub fn encode_command(&self) -> Vec<u8> {
        match self {
            Command::String(data) => [&[b'-', data.len() as u8][..], data].concat(), // It's safe to `data.len() as u8`, because max Command::String len is u8::MAX
            Command::Array(data) => {
                let mut result = vec![b'*', data.len() as u8]; // It's safe to `data.len() as u8`, because max Command::Array len is u8::MAX
                for command in data {
                    result.extend(command.encode_command());
                }
                result
            }
        }
    }
}

impl TryFrom<Bytes> for Command {
    type Error = CommandError;
    fn try_from(value: Bytes) -> Result<Self, Self::Error> {
        Self::parse_unknown(&value, 0).map(|(_, command)| command)
    }
}

impl From<std::io::Error> for CommandError {
    fn from(e: std::io::Error) -> CommandError {
        CommandError::IOError(e)
    }
}

impl CommandError {
    /// Encode CommandError variant through protocol idea
    pub fn encode_error(&self) -> Vec<u8> {
        match self {
            CommandError::UnknownCommandType(data) => {
                vec![b'!', 1, *data]
            }
            CommandError::InvalidCommandName(data) => {
                [&[b'!', data.len() as u8][..], data].concat()
            }
            CommandError::ExecutionError(data) => [&[b'!', data.len() as u8][..], data].concat(),
            CommandError::IOError(err) => {
                let error_msg = err.to_string().into_bytes();
                [&[b'!', error_msg.len() as u8][..], &error_msg].concat()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Command::*;
    use super::*;

    #[test]
    fn test_decode_simple() {
        let buf = Bytes::from(&[4, b'E', b'C', b'H', b'O'] as &[u8]);
        assert_eq!(
            Command::parse_simple(&buf, 0),
            (5, Bytes::from_static(b"ECHO"))
        );
    }

    #[test]
    fn test_decode_string() {
        let buf = Bytes::from(&[b'-', 4, b'E', b'C', b'H', b'O'] as &[u8]);
        assert_eq!(
            Command::parse_unknown(&buf, 0).unwrap(),
            (6, String(Bytes::from_static(b"ECHO")))
        );
    }

    #[test]
    fn test_decode_string_array() {
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

    #[test]
    fn test_encode_string() {
        assert_eq!(
            String(Bytes::from_static(b"ECHO")).encode_command(),
            vec![b'-', 4, b'E', b'C', b'H', b'O']
        );
    }

    #[test]
    fn test_encode_string_array() {
        assert_eq!(
            Array(vec![
                String(Bytes::from_static(b"ECHO")),
                String(Bytes::from_static(b"OK"))
            ])
            .encode_command(),
            vec![
                b'*', 2, b'-', 4, b'E', b'C', b'H', b'O', b'-', 2, b'O', b'K'
            ]
        );
    }
}
