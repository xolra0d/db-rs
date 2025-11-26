use crate::sql::sql_parser::LogicalPlan;

mod query_flattening;

impl LogicalPlan {
    pub fn optimize(self) -> LogicalPlan {
        self.flatten()
    }
}
