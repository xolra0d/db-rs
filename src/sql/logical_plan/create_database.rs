use crate::config::CONFIG;
use crate::error::{Error, Result};
use crate::sql::sql_parser::LogicalPlan;
use sqlparser::ast::{ObjectName, ObjectNamePart};

impl LogicalPlan {
    pub fn from_create_database(db_name: &ObjectName, if_not_exists: bool) -> Result<Self> {
        if db_name.0.len() != 1 {
            return Err(Error::InvalidDatabaseName);
        }

        let ObjectNamePart::Identifier(name) = &db_name.0[0] else {
            return Err(Error::InvalidDatabaseName);
        };

        let name = &name.value;
        if !Self::validate_name(name) {
            return Err(Error::InvalidDatabaseName);
        }

        let path = CONFIG.get_db_dir().join(name);
        let exists = path.exists();

        if exists && if_not_exists {
            return Ok(Self::Skip);
        }

        if exists {
            return Err(Error::DatabaseAlreadyExists);
        }

        Ok(Self::CreateDatabase {
            name: name.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use sqlparser::ast::Ident;

    fn build_from_string(name: &str) -> ObjectName {
        ObjectName(vec![ObjectNamePart::Identifier(Ident::new(
            name.to_string(),
        ))])
    }

    #[test]
    fn test_create_database_validation() {
        let mut invalid = build_from_string("invalid*");
        assert!(LogicalPlan::from_create_database(&invalid, false).is_err());
        assert!(LogicalPlan::from_create_database(&invalid, true).is_err());
        invalid = build_from_string("invalid`");
        assert!(LogicalPlan::from_create_database(&invalid, false).is_err());
        assert!(LogicalPlan::from_create_database(&invalid, true).is_err());
    }
}
