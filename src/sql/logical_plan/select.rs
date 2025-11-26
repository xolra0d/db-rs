use sqlparser::ast::{
    Expr, LimitClause, OrderByKind, Query, SelectItem, SetExpr, TableFactor, Value as SQLValue,
};

use crate::error::{Error, Result};
use crate::runtime_config::TABLE_DATA;
use crate::sql::parse_ident;
use crate::sql::sql_parser::{LogicalPlan, ScanSource};
use crate::storage::{ColumnDef, TableDef};

impl LogicalPlan {
    pub fn from_query(query: &Query) -> Result<Self> {
        // dbg!(&query);
        // return Ok(Self::Skip);

        let SetExpr::Select(select) = &*query.body else {
            return Err(Error::UnsupportedCommand(
                "Only SELECT queries are supported".to_string(),
            ));
        };

        if select.from.len() != 1 {
            return Err(Error::UnsupportedCommand(
                "Currently do not support multiple table selects".to_string(),
            ));
        }
        let table = &select.from[0];

        if !table.joins.is_empty() {
            return Err(Error::UnsupportedCommand(
                "JOIN clauses are not currently supported".to_string(),
            ));
        }

        let scan_source = match &table.relation {
            TableFactor::Table { name, .. } => {
                let table_def = TableDef::try_from(name)?;
                ScanSource::Table(table_def)
            }
            TableFactor::Derived { subquery, .. } => {
                let subquery_plan = Self::from_query(subquery)?;
                ScanSource::Subquery(Box::new(subquery_plan))
            }
            _ => {
                return Err(Error::UnsupportedCommand(
                    "Only simple table references and subqueries are supported".to_string(),
                ));
            }
        };

        if select.projection.is_empty() {
            return Err(Error::UnsupportedCommand(
                "No projection specified.".to_string(),
            ));
        }

        let mut plan = Self::Scan {
            source: scan_source,
        };

        let mut read_columns = Vec::with_capacity(select.projection.len());

        let available_columns = Self::extract_columns_from_plan(&plan)?;

        // Allow either
        // * Wildcard only, meaning all columns.
        // * Wildcard at the end, meaning all columns which are not specified.
        // * No wildcard.
        let mut wildcard = None;
        for (idx, projection) in select.projection.iter().enumerate() {
            match projection {
                SelectItem::Wildcard(_) => {
                    if wildcard.is_some() {
                        return Err(Error::UnsupportedCommand(
                            "Multiple wildcards are not supported".to_string(),
                        ));
                    }
                    wildcard = Some(idx);
                }
                SelectItem::UnnamedExpr(expr) => {
                    if wildcard.is_some() {
                        return Err(Error::UnsupportedCommand(
                            "Columns after wildcard are not supported".to_string(),
                        ));
                    }
                    let Expr::Identifier(ident) = expr else {
                        return Err(Error::UnsupportedCommand(
                            "Only column identifiers are supported in projections".to_string(),
                        ));
                    };

                    let column_def = parse_ident(ident, &available_columns)?;
                    if read_columns.contains(&column_def) {
                        return Err(Error::DuplicateColumn(ident.value.clone()));
                    }
                    read_columns.push(column_def);
                }
                _ => {
                    return Err(Error::UnsupportedCommand(
                        "Only simple column projections and wildcards are supported".to_string(),
                    ));
                }
            }
        }

        if let Some(idx) = wildcard {
            if idx == 0 {
                read_columns.clone_from(&available_columns);
            } else {
                for column in &available_columns {
                    if !read_columns.contains(column) {
                        read_columns.push(column.clone());
                    }
                }
            }
        }

        if let Some(ref selection) = select.selection {
            plan = LogicalPlan::Filter {
                expr: Box::new(selection.clone()),
                plan: Box::new(plan),
            };
        }

        plan = LogicalPlan::Projection {
            columns: read_columns.clone(),
            plan: Box::new(plan),
        };

        if let Some(order_by) = &query.order_by {
            match &order_by.kind {
                OrderByKind::All(_params) => {
                    plan = LogicalPlan::OrderBy {
                        column_defs: vec![read_columns], // todo save as Cow<> of projection maybe, or even indexes?
                        plan: Box::new(plan),
                    };
                }
                OrderByKind::Expressions(order_by_given) => {
                    let mut order_by_all = Vec::with_capacity(order_by_given.len());
                    for order_by_expr in order_by_given {
                        let order_by_cols =
                            Self::parse_primary_key(&order_by_expr.expr, &available_columns)?; // OrderBy cols is interpreted in the same way as PK in `CREATE TABLE`
                        order_by_all.push(order_by_cols);
                    }

                    plan = LogicalPlan::OrderBy {
                        column_defs: order_by_all,
                        plan: Box::new(plan),
                    };
                }
            }
        }

        if let Some(limit_clause) = &query.limit_clause {
            let LimitClause::LimitOffset {
                limit: limit_expr,
                offset: offset_expr,
                ..
            } = limit_clause
            else {
                return Err(Error::InvalidLimitValue(
                    "Only LIMIT OFFSET clause is supported".to_string(),
                ));
            };

            let mut limit = None;
            let mut offset = 0;

            if let Some(limit_expr) = limit_expr {
                let Expr::Value(limit_expr) = &limit_expr else {
                    return Err(Error::InvalidLimitValue(
                        "LIMIT must be a literal value".to_string(),
                    ));
                };
                let SQLValue::Number(limit_expr, _) = &limit_expr.value else {
                    return Err(Error::InvalidLimitValue(
                        "LIMIT must be a number".to_string(),
                    ));
                };
                limit = Some(
                    limit_expr
                        .parse()
                        .map_err(|_| Error::InvalidLimitValue(limit_expr.clone()))?,
                );
            }

            if let Some(offset_expr) = offset_expr {
                let Expr::Value(offset_expr) = &offset_expr.value else {
                    return Err(Error::InvalidLimitValue(
                        "OFFSET must be a literal value".to_string(),
                    ));
                };
                let SQLValue::Number(offset_expr, _) = &offset_expr.value else {
                    return Err(Error::InvalidLimitValue(
                        "OFFSET must be a number".to_string(),
                    ));
                };

                offset = offset_expr
                    .parse()
                    .map_err(|_| Error::InvalidLimitValue(offset_expr.clone()))?;
            }

            plan = LogicalPlan::Limit {
                limit,
                offset,
                plan: Box::new(plan),
            };
        }

        Ok(plan)
    }

    /// Extract column definitions from a logical plan
    fn extract_columns_from_plan(plan: &LogicalPlan) -> Result<Vec<ColumnDef>> {
        match plan {
            LogicalPlan::Projection { columns, .. } => Ok(columns.clone()),
            LogicalPlan::Filter { plan, .. }
            | LogicalPlan::OrderBy { plan, .. }
            | LogicalPlan::Limit { plan, .. } => Self::extract_columns_from_plan(plan),
            LogicalPlan::Scan { source } => match source {
                ScanSource::Table(table_def) => {
                    let Some(table_config) = TABLE_DATA.get(table_def) else {
                        return Err(Error::TableNotFound);
                    };
                    Ok(table_config.metadata.schema.columns.clone())
                }
                ScanSource::Subquery(subquery_plan) => {
                    Self::extract_columns_from_plan(subquery_plan)
                }
            },
            _ => Err(Error::UnsupportedCommand(
                "Cannot extract columns from this plan type".to_string(),
            )),
        }
    }
}
