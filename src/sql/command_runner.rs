use crate::error::Result;
use crate::sql::sql_parser::{LogicalPlan, PhysicalPlan};
use crate::storage::OutputTable;

/// Main runner struct which executes received command.
#[derive(Debug)]
pub struct CommandRunner;

impl CommandRunner {
    /// Handles full command execution.
    pub fn execute_command(command: &str) -> Result<OutputTable> {
        let logical_plan = LogicalPlan::try_from(command)?;

        let logical_plan = logical_plan.optimize_self();

        let physical_plan = PhysicalPlan::from(logical_plan);

        Self::execute_physical_plan(physical_plan)
    }

    /// Execution of the physical plan.
    fn execute_physical_plan(plan: PhysicalPlan) -> Result<OutputTable> {
        match plan {
            PhysicalPlan::Skip => Ok(OutputTable::build_ok()),
            PhysicalPlan::CreateDatabase { name } => Self::create_database(name),
            PhysicalPlan::CreateTable {
                name,
                columns,
                settings,
                order_by,
                primary_key,
            } => Self::create_table(name, columns, settings, order_by, primary_key),
            PhysicalPlan::Insert { table_def, columns } => Self::insert(table_def, columns),
            PhysicalPlan::Select {
                table_def,
                columns,
                filter,
            } => Self::select(table_def, columns, filter),
        }
    }
}
