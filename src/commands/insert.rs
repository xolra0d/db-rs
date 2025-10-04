use crate::commands::DatabaseCommand;
use crate::engine::{Engine, FieldType, TableSpecifier};
use crate::protocol::{Command, CommandError, CommandResult};

use crc32fast;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

/// Echoes back all provided arguments.
pub struct InsertCommand;

impl DatabaseCommand for InsertCommand {
    fn name() -> &'static str {
        "insert"
    }

    fn execute(args: &[Command], engine: &Engine) -> CommandResult<Command> {
        if args.len() < 6 {
            return Err(CommandError::ExecutionError(
                "Usage: insert <db_name> <table_name> fields <field_name_1> <field_name_2> values <value_1> <value_2> <value_3> <value_4>".into(),
            ));
        }

        let Command::String(db_name) = &args[0] else {
            return Err(CommandError::ExecutionError(
                "Database name must be a string".into(),
            ));
        };

        let Command::String(table_name) = &args[1] else {
            return Err(CommandError::ExecutionError(
                "Table name must be a string".into(),
            ));
        };

        let fields_start = args
            .iter()
            .position(|arg| {
                if let Command::String(s) = arg {
                    s == "fields"
                } else {
                    false
                }
            })
            .ok_or_else(|| {
                CommandError::ExecutionError("Expected 'fields' keyword in command".into())
            })?;

        let values_start = args
            .iter()
            .position(|arg| {
                if let Command::String(s) = arg {
                    s == "values"
                } else {
                    false
                }
            })
            .ok_or_else(|| {
                CommandError::ExecutionError("Expected 'values' keyword in command".into())
            })?;

        if values_start <= fields_start + 1 {
            return Err(CommandError::ExecutionError(
                "No field names provided between 'fields' and 'values' keywords".into(),
            ));
        }

        let field_names: Vec<String> = args[fields_start + 1..values_start]
            .iter()
            .map(|arg| {
                if let Command::String(s) = arg {
                    Ok(s.clone())
                } else {
                    Err(CommandError::ExecutionError(
                        "Field names must be strings".into(),
                    ))
                }
            })
            .collect::<Result<Vec<String>, CommandError>>()?;

        let values = &args[values_start + 1..];

        let table_specifier = TableSpecifier::new(db_name, Some(table_name));

        if !table_specifier.exists(engine) {
            return Err(CommandError::ExecutionError(
                "Database or table does not exist".into(),
            ));
        }

        let table_fields = engine.get_table_fields(&table_specifier)?;

        if field_names.len() != table_fields.len() {
            // TODO: add Option type to allow None values
            return Err(CommandError::ExecutionError(format!(
                "Number of field names ({}) does not match table field count ({})",
                field_names.len(),
                table_fields.len()
            )));
        }

        if !values.len().is_multiple_of(field_names.len()) {
            return Err(CommandError::ExecutionError(format!(
                "Number of values ({}) must be a multiple of field count ({})",
                values.len(),
                field_names.len()
            )));
        }

        engine.lock_table(table_specifier.clone())?;

        let output = insert_rows_with_field_mapping(
            engine,
            &table_specifier,
            &table_fields,
            &field_names,
            values,
        );

        engine.unlock_table(&table_specifier)?;

        output
    }

    fn description() -> &'static str {
        "Insert rows in the table with field names and values"
    }
}

fn command_to_bytes_rmp_crc(command: &Command) -> CommandResult<Vec<u8>> {
    let Ok(encoded_msg) = rmp_serde::to_vec(&command) else {
        return Err(CommandError::ExecutionError(format!(
            "Couldn't deserialize your data: {command:?}"
        )));
    };
    let length = (encoded_msg.len() as u32).to_le_bytes();
    let checksum = crc32fast::hash(&encoded_msg).to_le_bytes();

    let mut result = Vec::with_capacity(4 + encoded_msg.len() + 4);
    result.extend(length);
    result.extend(encoded_msg);
    result.extend(checksum);
    Ok(result)
}

fn insert_rows_with_field_mapping(
    engine: &Engine,
    table_specifier: &TableSpecifier,
    table_fields: &[(String, FieldType)],
    field_names: &[String],
    values: &[Command],
) -> CommandResult<Command> {
    let fields_count = field_names.len();
    let mut to_insert: HashMap<String, Vec<u8>> = HashMap::with_capacity(fields_count);

    // Create a mapping from field names to their types
    let field_type_map: HashMap<String, FieldType> = table_fields
        .iter()
        .map(|(name, field_type)| (name.clone(), field_type.clone()))
        .collect();

    for field_name in field_names {
        if !field_type_map.contains_key(field_name) {
            return Err(CommandError::ExecutionError(format!(
                "Field '{}' does not exist in table",
                field_name
            )));
        }
    }

    for chunk in values.chunks(fields_count) {
        for (i, command) in chunk.iter().enumerate() {
            let field_name = &field_names[i];
            let field_type = &field_type_map[field_name];

            if &FieldType::get_field_type_from_command(command) != field_type {
                return Err(CommandError::ExecutionError(format!(
                    "Wrong field type for field '{}'. Expected: {:?}, received: {:?}",
                    field_name, field_type, command
                )));
            }

            to_insert
                .entry(field_name.clone())
                .or_default()
                .extend(command_to_bytes_rmp_crc(command)?);
        }
    }

    let path = engine.get_db_dir().join(PathBuf::from(table_specifier));

    for (field_name, values) in to_insert {
        let field_type = table_fields
            .iter()
            .find(|(name, _)| name == &field_name)
            .map(|(_, field_type)| field_type)
            .ok_or_else(|| {
                CommandError::ExecutionError(format!(
                    "Field '{}' not found in table fields",
                    field_name
                ))
            })?;

        let file_name = format!("{}.{}", field_name, field_type.to_str());

        let mut file = OpenOptions::new()
            .append(true)
            .open(path.join(&file_name))?;

        file.write_all(&values)?;
    }

    Ok(Command::String(String::from("OK")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn prepare_engine(dir_name: &str) -> (PathBuf, Engine) {
        let temp_dir = std::env::temp_dir().join(dir_name);
        std::fs::create_dir_all(&temp_dir).unwrap();

        let engine = Engine::new(temp_dir.clone());

        (temp_dir, engine)
    }

    fn create_test_database_and_table(
        engine: &Engine,
        db_name: &str,
        table_name: &str,
    ) -> Result<(), CommandError> {
        let create_db_args = vec![
            Command::String("database".to_string()),
            Command::String(db_name.to_string()),
        ];
        crate::commands::create::CreateCommand::execute(&create_db_args, engine)?;

        let create_table_args = vec![
            Command::String("table".to_string()),
            Command::String(db_name.to_string()),
            Command::String(table_name.to_string()),
            Command::String("name".to_string()),
            Command::String("String".to_string()),
            Command::String("age".to_string()),
            Command::String("String".to_string()),
        ];
        crate::commands::create::CreateCommand::execute(&create_table_args, engine)?;

        Ok(())
    }

    #[test]
    fn test_insert_success_multiple_rows() {
        let (temp_dir, engine) = prepare_engine("test_insert_success_multiple");

        create_test_database_and_table(&engine, "testdb_multiple", "users").unwrap();

        let insert_args = vec![
            Command::String("testdb_multiple".to_string()),
            Command::String("users".to_string()),
            Command::String("fields".to_string()),
            Command::String("name".to_string()),
            Command::String("age".to_string()),
            Command::String("values".to_string()),
            Command::String("John".to_string()),
            Command::String("25".to_string()),
            Command::String("Jane".to_string()),
            Command::String("30".to_string()),
            Command::String("Bob".to_string()),
            Command::String("35".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine).unwrap();
        assert_eq!(result, Command::String("OK".to_string()));

        let table_path = temp_dir.join("testdb_multiple").join("users");
        let name_file = table_path.join("name.String");
        let age_file = table_path.join("age.String");

        let mut name_expected: Vec<u8> = Vec::new();
        name_expected
            .extend(command_to_bytes_rmp_crc(&Command::String("John".to_string())).unwrap());
        name_expected
            .extend(command_to_bytes_rmp_crc(&Command::String("Jane".to_string())).unwrap());
        name_expected
            .extend(command_to_bytes_rmp_crc(&Command::String("Bob".to_string())).unwrap());

        let name_content = std::fs::read(&name_file).unwrap();
        assert_eq!(name_content, name_expected);

        let mut age_expected: Vec<u8> = Vec::new();
        age_expected.extend(command_to_bytes_rmp_crc(&Command::String("25".to_string())).unwrap());
        age_expected.extend(command_to_bytes_rmp_crc(&Command::String("30".to_string())).unwrap());
        age_expected.extend(command_to_bytes_rmp_crc(&Command::String("35".to_string())).unwrap());

        let age_content = std::fs::read(&age_file).unwrap();
        assert_eq!(age_content, age_expected);

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_too_few_arguments() {
        let (temp_dir, engine) = prepare_engine("test_insert_too_few_args");

        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Usage: insert"));
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_missing_fields_keyword() {
        let (temp_dir, engine) = prepare_engine("test_insert_missing_fields");

        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
            Command::String("name".to_string()),
            Command::String("values".to_string()),
            Command::String("John".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Usage: insert")); // When fields keyword is missing, it returns usage message
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_missing_values_keyword() {
        let (temp_dir, engine) = prepare_engine("test_insert_missing_values");

        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
            Command::String("fields".to_string()),
            Command::String("name".to_string()),
            Command::String("John".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Usage: insert")); // When values keyword is missing, it returns usage message
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_no_field_names() {
        let (temp_dir, engine) = prepare_engine("test_insert_no_field_names");

        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
            Command::String("fields".to_string()),
            Command::String("values".to_string()),
            Command::String("John".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Usage: insert")); // When no field names are provided between fields and values, it returns usage message
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_field_count_mismatch() {
        let (temp_dir, engine) = prepare_engine("test_insert_field_count_mismatch");

        create_test_database_and_table(&engine, "testdb", "users").unwrap();

        // Try to insert with only 1 field
        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
            Command::String("fields".to_string()),
            Command::String("age".to_string()),
            Command::String("values".to_string()),
            Command::String("25".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Number of field names"));
            assert!(msg.contains("does not match table field count"));
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_nonexistent_field() {
        let (temp_dir, engine) = prepare_engine("test_insert_nonexistent_field");

        create_test_database_and_table(&engine, "testdb", "users").unwrap();

        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
            Command::String("fields".to_string()),
            Command::String("nonexistent".to_string()),
            Command::String("age".to_string()),
            Command::String("values".to_string()),
            Command::String("John".to_string()),
            Command::String("25".to_string()),
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Field 'nonexistent' does not exist in table"));
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }

    #[test]
    fn test_insert_wrong_field_type() {
        let (temp_dir, engine) = prepare_engine("test_insert_wrong_field_type");

        create_test_database_and_table(&engine, "testdb", "users").unwrap();

        let insert_args = vec![
            Command::String("testdb".to_string()),
            Command::String("users".to_string()),
            Command::String("fields".to_string()),
            Command::String("name".to_string()),
            Command::String("age".to_string()),
            Command::String("values".to_string()),
            Command::String("John".to_string()),
            Command::Array(vec![Command::String("25".to_string())]), // should be a `Command::String`
        ];

        let result = InsertCommand::execute(&insert_args, &engine);
        assert!(result.is_err());
        if let Err(CommandError::ExecutionError(msg)) = result {
            assert!(msg.contains("Wrong field type for field 'age'"));
            assert!(msg.contains("Expected: String"));
        } else {
            panic!("Expected ExecutionError");
        }

        let _ = std::fs::remove_dir_all(&temp_dir);
    }
}
