use crate::system_tables::BatchIterator;
use crate::{
    query_log::{QueryLog, QueryLogEntry},
    system_tables::IoxSystemTable,
};
use arrow::{
    array::{DurationNanosecondArray, StringArray, TimestampNanosecondArray},
    datatypes::{DataType, Field, Schema, SchemaRef, TimeUnit},
    error::Result,
    record_batch::RecordBatch,
};
use observability_deps::tracing::error;
use std::{collections::VecDeque, sync::Arc};

/// Implementation of system.queries table
#[derive(Debug)]
pub(super) struct QueriesTable {
    schema: SchemaRef,
    query_log: Arc<QueryLog>,
}

impl QueriesTable {
    pub(super) fn new(query_log: Arc<QueryLog>) -> Self {
        Self {
            schema: queries_schema(),
            query_log,
        }
    }
}

impl IoxSystemTable for QueriesTable {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }

    fn scan(&self, batch_size: usize) -> Result<BatchIterator> {
        let schema = self.schema();
        let entries = self.query_log.entries();
        let mut offset = 0;
        Ok(Box::new(std::iter::from_fn(move || {
            if offset >= entries.len() {
                return None;
            }

            let len = batch_size.min(entries.len() - offset);
            match from_query_log_entries(schema.clone(), &entries, offset, len) {
                Ok(batch) => {
                    offset += len;
                    Some(Ok(batch))
                }
                Err(e) => {
                    error!("Error system.chunks table: {:?}", e);
                    Some(Err(e))
                }
            }
        })))
    }
}

fn queries_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(
            "issue_time",
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            false,
        ),
        Field::new("query_type", DataType::Utf8, false),
        Field::new("query_text", DataType::Utf8, false),
        Field::new(
            "completed_duration",
            DataType::Duration(TimeUnit::Nanosecond),
            false,
        ),
    ]))
}

fn from_query_log_entries(
    schema: SchemaRef,
    entries: &VecDeque<Arc<QueryLogEntry>>,
    offset: usize,
    len: usize,
) -> Result<RecordBatch> {
    let issue_time = entries
        .iter()
        .skip(offset)
        .take(len)
        .map(|e| e.issue_time)
        .map(|ts| Some(ts.timestamp_nanos()))
        .collect::<TimestampNanosecondArray>();

    let query_type = entries
        .iter()
        .skip(offset)
        .take(len)
        .map(|e| Some(&e.query_type))
        .collect::<StringArray>();

    let query_text = entries
        .iter()
        .skip(offset)
        .take(len)
        .map(|e| Some(e.query_text.to_string()))
        .collect::<StringArray>();

    let query_runtime = entries
        .iter()
        .skip(offset)
        .take(len)
        .map(|e| e.query_completed_duration().map(|d| d.as_nanos() as i64))
        .collect::<DurationNanosecondArray>();

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(issue_time),
            Arc::new(query_type),
            Arc::new(query_text),
            Arc::new(query_runtime),
        ],
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_util::assert_batches_eq;
    use time::{Time, TimeProvider};

    #[test]
    fn test_from_query_log() {
        let now = Time::from_rfc3339("1996-12-19T16:39:57+00:00").unwrap();
        let time_provider = Arc::new(time::MockProvider::new(now));

        let query_log = QueryLog::new(10, Arc::clone(&time_provider) as Arc<dyn TimeProvider>);
        query_log.push("sql", Box::new("select * from foo"));
        time_provider.inc(std::time::Duration::from_secs(24 * 60 * 60));
        query_log.push("sql", Box::new("select * from bar"));
        let read_filter_entry = query_log.push("read_filter", Box::new("json goop"));

        let table = QueriesTable::new(Arc::new(query_log));

        let expected = vec![
            "+----------------------+-------------+-------------------+--------------------+",
            "| issue_time           | query_type  | query_text        | completed_duration |",
            "+----------------------+-------------+-------------------+--------------------+",
            "| 1996-12-19T16:39:57Z | sql         | select * from foo |                    |",
            "| 1996-12-20T16:39:57Z | sql         | select * from bar |                    |",
            "| 1996-12-20T16:39:57Z | read_filter | json goop         |                    |",
            "+----------------------+-------------+-------------------+--------------------+",
        ];

        let entries = table.scan(3).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 1);
        assert_batches_eq!(&expected, &entries);

        // mark one of the queries completed after 4s
        let now = Time::from_rfc3339("1996-12-20T16:40:01+00:00").unwrap();
        read_filter_entry.set_completed(now);

        let expected = vec![
            "+----------------------+-------------+-------------------+--------------------+",
            "| issue_time           | query_type  | query_text        | completed_duration |",
            "+----------------------+-------------+-------------------+--------------------+",
            "| 1996-12-19T16:39:57Z | sql         | select * from foo |                    |",
            "| 1996-12-20T16:39:57Z | sql         | select * from bar |                    |",
            "| 1996-12-20T16:39:57Z | read_filter | json goop         | 4s                 |",
            "+----------------------+-------------+-------------------+--------------------+",
        ];

        let entries = table.scan(2).unwrap().collect::<Result<Vec<_>>>().unwrap();
        assert_eq!(entries.len(), 2);
        assert_batches_eq!(&expected, &entries);
    }
}
