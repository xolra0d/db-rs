use crate::runtime_config::TABLE_DATA;
use crate::storage::{Column, TableDef, TablePart, TablePartInfo, Value};
use log::{error, info, warn};
use uuid::Uuid;

pub struct BackgroundMerge;

impl BackgroundMerge {
    pub fn start() {
        info!("Background merges started");
        loop {
            let Some(merge_data) = find_two_parts() else {
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            };

            let part_0_cols = Self::load_part(&merge_data.table_def, &merge_data.part_0);
            let part_1_cols = Self::load_part(&merge_data.table_def, &merge_data.part_1);
            let merged = Self::merge_parts(part_0_cols, part_1_cols);

            let mut new_part = TablePart::try_new(
                &merge_data.table_def,
                merged,
                Some(merge_data.part_1.name.clone()),
            ) // use latest name of two for proper future merging
            .unwrap();
            new_part.save_raw(&merge_data.table_def).unwrap();

            // prevent from new selects
            let Some(mut config) = TABLE_DATA.get_mut(&merge_data.table_def) else {
                std::thread::sleep(std::time::Duration::from_secs(1));
                continue;
            };
            let part_0_old = merge_data
                .table_def
                .get_path()
                .join(&merge_data.part_0.name);
            let part_0_new = merge_data
                .table_def
                .get_path()
                .join(format!("{}.old", &merge_data.part_0.name));
            let part_1_old = merge_data
                .table_def
                .get_path()
                .join(&merge_data.part_1.name);
            let part_1_new = merge_data
                .table_def
                .get_path()
                .join(format!("{}.old", &merge_data.part_1.name));

            if std::fs::rename(&part_0_old, &part_0_new).is_err() {
                continue;
            }
            if std::fs::rename(&part_1_old, &part_1_new).is_err() {
                if let Err(error) = std::fs::rename(&part_0_new, part_0_old) {
                    error!(
                        "Couldn't move part ({}). Remove `.old` extension and solve the issue: {}",
                        part_0_new.display(),
                        error
                    );
                }
                continue;
            }
            config
                .infos
                .retain(|x| x.name != merge_data.part_0.name && x.name != merge_data.part_1.name);
            drop(config); // drop mut access for `move_to_normal`

            if new_part.move_to_normal(&merge_data.table_def).is_err() {
                let Some(mut config) = TABLE_DATA.get_mut(&merge_data.table_def) else {
                    continue;
                };
                if let Err(error) = std::fs::rename(&part_0_new, &part_0_old) {
                    error!(
                        "Couldn't move part ({}). Remove `.old` extension and solve the issue: {}",
                        part_0_new.display(),
                        error
                    );
                } else {
                    config.infos.push(merge_data.part_0);
                }
                if let Err(error) = std::fs::rename(&part_1_new, &part_1_old) {
                    error!(
                        "Couldn't move part ({}). Remove `.old` extension and solve the issue: {}",
                        part_1_new.display(),
                        error
                    );
                } else {
                    config.infos.push(merge_data.part_1);
                }
                continue;
            }

            if let Err(error) = std::fs::remove_dir_all(&part_0_new) {
                warn!(
                    "Couldn't remove ({}). Remove directory and solve the issue: {}",
                    part_0_new.display(),
                    error
                );
            }
            if let Err(error) = std::fs::remove_dir_all(&part_1_new) {
                warn!(
                    "Couldn't remove ({}). Remove directory and solve the issue: {}",
                    part_1_new.display(),
                    error
                );
            }
        }
    }

    fn load_part(table_def: &TableDef, part: &TablePartInfo) -> Vec<Column> {
        let mut columns = Vec::new();

        // column-stored version
        let mut marks = vec![Vec::new(); part.column_defs.len()];
        for mark in &part.marks {
            for (mark_idx, mark_info) in mark.info.iter().enumerate() {
                marks[mark_idx].push(mark_info.clone());
            }
        }
        for (col_idx, column_def) in part.column_defs.iter().enumerate() {
            let val = part
                .read_column(table_def, column_def, marks[col_idx].as_slice())
                .unwrap();
            columns.push(val);
        }
        columns
    }

    fn merge_parts(mut part_0: Vec<Column>, part_1: Vec<Column>) -> Vec<Column> {
        for column_1 in part_1 {
            if let Some(position) = part_0
                .iter()
                .position(|col| col.column_def == column_1.column_def)
            {
                part_0[position].data.extend(column_1.data.into_iter());
            } else {
                let mut data = vec![Value::Null; part_0[0].data.len()];
                data.extend(column_1.data.into_iter());
                part_0.push(Column {
                    column_def: column_1.column_def.clone(),
                    data,
                });
            }
        }

        part_0
    }
}

#[derive(Debug)]
struct MergeData {
    table_def: TableDef,
    part_0: TablePartInfo,
    part_1: TablePartInfo,
}

fn find_two_parts() -> Option<MergeData> {
    let data = TABLE_DATA.iter().find(|x| x.infos.len() > 1)?;

    let mut names: Vec<_> = data.infos.iter().map(|x| &x.name).collect();
    names.sort_by(|a, b| uuid_str_cmp(a, b));

    let part_0 = data.infos.iter().find(|x| x.name == *names[0])?;
    let part_1 = data.infos.iter().find(|x| x.name == *names[1])?;

    Some(MergeData {
        table_def: data.pair().0.clone(),
        part_0: part_0.clone(),
        part_1: part_1.clone(),
    })
}

/// Orders UUID by timestamps with seconds and nanoseconds
fn uuid_str_cmp(t1: &str, t2: &str) -> std::cmp::Ordering {
    if t1 == t2 {
        return std::cmp::Ordering::Equal;
    }

    // (seconds, subsec_nanos)
    let t1_unix = Uuid::parse_str(t1)
        .unwrap()
        .get_timestamp()
        .unwrap()
        .to_unix();
    let t2_unix = Uuid::parse_str(t2)
        .unwrap()
        .get_timestamp()
        .unwrap()
        .to_unix();

    if (t1_unix.0 > t2_unix.0) || (t1_unix.0 == t2_unix.0 && t1_unix.1 > t2_unix.1) {
        std::cmp::Ordering::Greater
    } else {
        std::cmp::Ordering::Less
    }
}
