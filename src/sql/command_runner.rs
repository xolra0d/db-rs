use crate::error::Result;
use crate::runtime_config::{ComplexityGuard, DATABASE_LOAD};
use crate::sql::sql_parser::{LogicalPlan, PhysicalPlan};
use crate::storage::OutputTable;

/// Main runner struct which executes received command.
#[derive(Debug)]
pub struct CommandRunner;

impl CommandRunner {
    /// Handles full command execution.
    pub fn execute_command(command: &str) -> Result<OutputTable> {
        let logical_plan = LogicalPlan::try_from(command)?;

        let logical_plan = logical_plan.optimize();

        let physical_plan = PhysicalPlan::from(logical_plan);

        let complexity = physical_plan.get_complexity();
        DATABASE_LOAD.fetch_add(complexity, std::sync::atomic::Ordering::Relaxed);
        let _guard = ComplexityGuard { complexity };

        Self::execute_physical_plan(physical_plan)
    }

    /// Execution of the physical plan.
    pub fn execute_physical_plan(plan: PhysicalPlan) -> Result<OutputTable> {
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
            PhysicalPlan::Insert { table_def, columns } => Self::insert(&table_def, columns),
            PhysicalPlan::DropDatabase { name, if_exists } => Self::drop_database(&name, if_exists),
            PhysicalPlan::DropTable { name, if_exists } => Self::drop_table(&name, if_exists),
            PhysicalPlan::Select {
                scan_source,
                columns,
                filter,
                sort_by,
                limit,
                offset,
            } => Self::select(
                scan_source,
                &columns,
                filter,
                sort_by.as_ref(),
                limit,
                offset,
            ),
        }
    }
}
