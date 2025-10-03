use crate::commands::DatabaseCommand;
use crate::engine::{Engine, FieldType, TableSpecifier};
use crate::protocol::{Command, CommandError, CommandResult};

use std::path::{Path, PathBuf};

/// Command for creating databases and tables.
/// ### Examples
/// - To create database: "create database `<database_name>`"
/// - To create table: "create table `<database_name>` `<table_name>` `<field_name:String>` `<field_type:String>` `<field_name:String>` `<field_type:String>`..."
pub struct CreateCommand;

/// Validation constants
const MIN_NAME_LENGTH: usize = 1;
const MAX_NAME_LENGTH: usize = 64;
const ALLOWED_NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_";

/// Validates database or table name
fn validate_name(name: &str) -> CommandResult<()> {
    if name.len() < MIN_NAME_LENGTH {
        return Err(CommandError::ExecutionError(format!(
            "Name must be at least {MIN_NAME_LENGTH} characters long"
        )));
    }

    if name.len() > MAX_NAME_LENGTH {
        return Err(CommandError::ExecutionError(format!(
            "Name must be at most {MAX_NAME_LENGTH} characters long"
        )));
    }

    if !name.chars().all(|c| ALLOWED_NAME_CHARS.contains(c)) {
        return Err(CommandError::ExecutionError(
            "Name can only contain alphanumeric characters and underscores".into(),
        ));
    }

    if name.starts_with('_') {
        return Err(CommandError::ExecutionError(
            "Name cannot start with underscore".into(),
        ));
    }

    Ok(())
}

impl DatabaseCommand for CreateCommand {
    fn name() -> &'static str {
        "create"
    }

    fn execute(args: &[Command], engine: &Engine) -> CommandResult<Command> {
        if args.len() < 2 {
            return Err(CommandError::ExecutionError(
                "Usage: create database <name> OR create table <db_name> <table_name> [field_name:field_type...]".into(),
            ));
        }

        let Command::String(data) = &args[0] else {
            return Err(CommandError::ExecutionError(
                "First argument must be a string (database or table)".into(),
            ));
        };

        match data.as_str() {
            "database" => {
                if args.len() != 2 {
                    return Err(CommandError::ExecutionError(
                        "Usage: create database <name>".into(),
                    ));
                }
                create_db(&args[1], engine)
            }
            "table" => {
                if args.len() < 3 {
                    return Err(CommandError::ExecutionError(
                        "Usage: create table <db_name> <table_name> [field_name:field_type...]"
                            .into(),
                    ));
                }
                create_table(&args[1..], engine)
            }
            arg => Err(CommandError::ExecutionError(format!(
                "Expected 'database' or 'table', got: '{arg}'"
            ))),
        }
    }

    fn description() -> &'static str {
        "Create a new database or table. Usage: create database <name> OR create table <db_name> <table_name> [field_name:field_type...]"
    }
}

fn create_db(db_name: &Command, engine: &Engine) -> CommandResult<Command> {
    let Command::String(db_name) = db_name else {
        return Err(CommandError::ExecutionError(
            "Database name must be a string".into(),
        ));
    };
    validate_name(db_name)?;

    let db_specifier = TableSpecifier::new(db_name, None);

    match std::fs::create_dir(engine.get_db_dir().join(PathBuf::from(db_specifier))) {
        Ok(()) => Ok(Command::String("OK".into())),
        Err(error) => match error.kind() {
            std::io::ErrorKind::AlreadyExists => Err(CommandError::ExecutionError(format!(
                "Database '{db_name}' already exists"
            ))),
            std::io::ErrorKind::PermissionDenied => Err(CommandError::ExecutionError(format!(
                "Permission denied to create database '{db_name}'"
            ))),
            std::io::ErrorKind::InvalidInput => Err(CommandError::ExecutionError(format!(
                "Invalid database name '{db_name}'"
            ))),
            _ => Err(CommandError::ExecutionError(format!(
                "Failed to create database '{db_name}': {error}"
            ))),
        },
    }
}

fn create_table(args: &[Command], engine: &Engine) -> CommandResult<Command> {
    let Command::String(db_name) = &args[0] else {
        return Err(CommandError::ExecutionError(
            "Database name must be a string".into(),
        ));
    };
    validate_name(db_name)?;

    // Parse table name
    let Command::String(table_name) = &args[1] else {
        return Err(CommandError::ExecutionError(
            "Table name must be a string".into(),
        ));
    };
    validate_name(table_name)?;

    let table_specifier = TableSpecifier::new(db_name, Some(table_name));

    engine.lock_table(table_specifier.clone())?;

    if table_specifier.exists(engine) {
        return Err(CommandError::ExecutionError(format!(
            "Table '{table_name}' already exists in database '{db_name}'",
        )));
    }

    let fields = parse_fields(&args[2..])?;
    if fields.is_empty() {
        return Err(CommandError::ExecutionError(
            "Table must have at least one field".into(),
        ));
    }

    let path = engine.get_db_dir().join(PathBuf::from(&table_specifier));

    if let Err(error) = std::fs::create_dir(&path) {
        engine.unlock_table(&table_specifier)?;
        return Err(CommandError::IOError(error.to_string()));
    }

    if let Err(error) = create_field_files(&path, &fields) {
        engine.unlock_table(&table_specifier)?;
        return Err(error);
    }

    engine.unlock_table(&table_specifier)?;

    Ok(Command::String("OK".into()))
}

/// Parse field definitions from command arguments
fn parse_fields(args: &[Command]) -> CommandResult<Vec<(String, FieldType)>> {
    if !args.len().is_multiple_of(2) {
        return Err(CommandError::ExecutionError(
            "Field definitions must come in pairs (name, type)".into(),
        ));
    }

    let mut fields = Vec::with_capacity(args.len() / 2);

    for chunk in args.chunks_exact(2) {
        let Command::String(field_name) = &chunk[0] else {
            return Err(CommandError::ExecutionError(
                "Field name must be a string".into(),
            ));
        };
        validate_name(field_name)?;

        let Command::String(field_type_str) = &chunk[1] else {
            return Err(CommandError::ExecutionError(
                "Field type must be a string".into(),
            ));
        };
        let field_type = FieldType::parse_field_type_from_str(field_type_str);

        let Some(field_type) = field_type else {
            return Err(CommandError::ExecutionError(format!(
                "Unknown field type: {field_type_str}"
            )));
        };

        // Check for duplicate field names
        if fields.iter().any(|(name, _)| name == field_name) {
            return Err(CommandError::ExecutionError(format!(
                "Duplicate field name: '{field_name}'"
            )));
        }

        fields.push((field_name.to_string(), field_type));
    }

    Ok(fields)
}

fn create_field_files(table_path: &Path, fields: &[(String, FieldType)]) -> CommandResult<()> {
    for (field_name, field_type) in fields {
        let field_type_str = field_type.to_str();
        let file_path = table_path.join(format!("{field_name}.{field_type_str}"));

        std::fs::File::create(&file_path).map_err(|e| {
            CommandError::ExecutionError(format!(
                "Failed to create field file '{}': {}",
                file_path.display(),
                e
            ))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn prepare_engine(dir_name: &str) -> (PathBuf, Engine) {
        let temp_dir = std::env::temp_dir().join(dir_name);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let engine = Engine::new(temp_dir.clone());

        (temp_dir, engine)
    }

    #[test]
    fn test_create_database() {
        let (temp_dir, engine) = prepare_engine("test_db_create_database");

        let args = vec![
            Command::String(String::from("database")),
            Command::String(String::from("testdb")),
        ];

        let result = CreateCommand::execute(&args, &engine).unwrap();

        assert!(matches!(result, Command::String(_)));

        assert!(temp_dir.join("testdb").exists());
        assert!(temp_dir.join("testdb").is_dir());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_create_table_success() {
        let (temp_dir, engine) = prepare_engine("test_db_create_table");

        // First create database
        let args = vec![
            Command::String(String::from("database")),
            Command::String(String::from("testdb")),
        ];

        let result = CreateCommand::execute(&args, &engine).unwrap();
        assert!(matches!(result, Command::String(_)));

        let test_db_path = temp_dir.join("testdb");
        assert!(test_db_path.exists());
        assert!(test_db_path.is_dir());

        // Then create table
        let args = vec![
            Command::String(String::from("table")),
            Command::String(String::from("testdb")),
            Command::String(String::from("test_table")),
            Command::String(String::from("name")),
            Command::String(String::from("String")),
            Command::String(String::from("job")),
            Command::String(String::from("String")),
        ];

        let result = CreateCommand::execute(&args, &engine).unwrap();
        assert!(matches!(result, Command::String(_)));

        let table_path = test_db_path.join("test_table");
        assert!(table_path.exists());
        assert!(table_path.is_dir());

        // Check field files were created
        assert!(table_path.join("name.String").exists());
        assert!(table_path.join("job.String").exists());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_create_database_validation() {
        let (temp_dir, engine) = prepare_engine("test_db_create_validation");

        // Test empty name
        let args = vec![
            Command::String(String::from("database")),
            Command::String(String::new()),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        // Test invalid characters
        let args = vec![
            Command::String(String::from("database")),
            Command::String(String::from("test-db")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        // Test name starting with underscore
        let args = vec![
            Command::String(String::from("database")),
            Command::String(String::from("_testdb")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_create_table_duplicate_fields() {
        let (temp_dir, engine) = prepare_engine("test_db_duplicate_fields");

        // Create database first
        let args = vec![
            Command::String(String::from("database")),
            Command::String(String::from("testdb")),
        ];
        CreateCommand::execute(&args, &engine).unwrap();

        // Try to create table with duplicate field names
        let args = vec![
            Command::String(String::from("table")),
            Command::String(String::from("testdb")),
            Command::String(String::from("test_table")),
            Command::String(String::from("name")),
            Command::String(String::from("String")),
            Command::String(String::from("name")), // Duplicate!
            Command::String(String::from("String")),
        ];
        assert!(CreateCommand::execute(&args, &engine).is_err());

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
