mod command_runner;
mod execution;
mod logical_plan;
mod sql_parser;

pub use command_runner::CommandRunner;

/// Validate the name of fields, databases, columns.
/// Returns true if `name` consists of english alphabet, numbers and underscore
/// Otherwise returns false
pub fn validate_name(name: &str) -> bool {
    name.chars()
        .all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

#[cfg(test)]
pub mod tests {
    use crate::sql::validate_name;

    #[test]
    fn test_invalid_names() {
        assert!(!validate_name("*"));
        assert!(!validate_name("csji="));
        assert!(!validate_name("csji122yrd01/"));
    }

    #[test]
    fn test_valid_names() {
        assert!(validate_name("coffee_shop"));
        assert!(validate_name("amsterdam"));
        assert!(validate_name("John_Data"));
    }
}
