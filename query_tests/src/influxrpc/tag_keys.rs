use data_types::timestamp::{MAX_NANO_TIME, MIN_NANO_TIME};
use datafusion::logical_plan::{col, lit};
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::PredicateBuilder;
use query::{
    exec::stringset::{IntoStringSet, StringSetRef},
    frontend::influxrpc::InfluxRpcPlanner,
};

use crate::scenarios::*;

/// Creates and loads several database scenarios using the db_setup
/// function.
///
/// runs table_column_names(predicate) and compares it to the expected
/// output
async fn run_tag_keys_test_case<D>(
    db_setup: D,
    predicate: InfluxRpcPredicate,
    expected_tag_keys: Vec<&str>,
) where
    D: DbSetup,
{
    test_helpers::maybe_start_logging();

    for scenario in db_setup.make().await {
        let DbScenario {
            scenario_name, db, ..
        } = scenario;
        println!("Running scenario '{}'", scenario_name);
        println!("Predicate: '{:#?}'", predicate);
        let planner = InfluxRpcPlanner::new();
        let ctx = db.executor().new_context(query::exec::ExecutorType::Query);

        let plan = planner
            .tag_keys(db.as_ref(), predicate.clone())
            .expect("built plan successfully");
        let names = ctx
            .to_string_set(plan)
            .await
            .expect("converted plan to strings successfully");

        assert_eq!(
            names,
            to_stringset(&expected_tag_keys),
            "Error in  scenario '{}'\n\nexpected:\n{:?}\nactual:\n{:?}",
            scenario_name,
            expected_tag_keys,
            names
        );
    }
}

#[tokio::test]
async fn list_tag_columns_with_no_tags() {
    run_tag_keys_test_case(
        OneMeasurementNoTags {},
        InfluxRpcPredicate::default(),
        vec![],
    )
    .await;

    let predicate = PredicateBuilder::default().timestamp_range(0, 1000).build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    run_tag_keys_test_case(OneMeasurementNoTags {}, predicate, vec![]).await;
}

#[tokio::test]
async fn list_tag_columns_no_predicate() {
    let expected_tag_keys = vec!["borough", "city", "county", "state"];
    run_tag_keys_test_case(
        TwoMeasurementsManyNulls {},
        InfluxRpcPredicate::default(),
        expected_tag_keys,
    )
    .await;
}

// NGA todo: add delete tests when TwoMeasurementsManyNullsWithDelete available

#[tokio::test]
async fn list_tag_columns_timestamp() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(150, 201)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_predicate() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "county", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_pred() {
    // Select only the following line using a _measurement predicate
    //
    // "o2,state=NY,city=NYC temp=61.0 500",
    let predicate = PredicateBuilder::default()
        .timestamp_range(450, 550)
        .add_expr(col("_measurement").eq(lit("o2"))) // _measurement=o2
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_timestamp_and_predicate() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(150, 201)
        .add_expr(col("state").eq(lit("MA"))) // state=MA
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name() {
    let predicate = InfluxRpcPredicate::new_table("o2", Default::default());
    let expected_tag_keys = vec!["borough", "city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_timestamp() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(150, 201)
        .build();
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_predicate() {
    let predicate = PredicateBuilder::default()
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
    let expected_tag_keys = vec!["borough", "city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_columns_measurement_name_and_predicate_and_timestamp() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(1, 550)
        .add_expr(col("state").eq(lit("NY"))) // state=NY
        .build();
    let predicate = InfluxRpcPredicate::new_table("o2", predicate);
    let expected_tag_keys = vec!["city", "state"];
    run_tag_keys_test_case(TwoMeasurementsManyNulls {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_end_to_end() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(0, 10000)
        .add_expr(col("host").eq(lit("server01")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host", "name", "region"];
    run_tag_keys_test_case(EndToEndTest {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_end_to_end_with_delete() {
    let predicate = PredicateBuilder::default()
        .timestamp_range(0, 10000)
        .add_expr(col("host").eq(lit("server01")))
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host", "region"];
    run_tag_keys_test_case(EndToEndTestWithDelete {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_time() {
    test_helpers::maybe_start_logging();
    let predicate = PredicateBuilder::default()
        .timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host"];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_i64() {
    test_helpers::maybe_start_logging();
    let predicate = PredicateBuilder::default()
        // outside valid timestamp range
        .timestamp_range(i64::MIN, i64::MAX)
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec!["host"];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_time_less_one() {
    test_helpers::maybe_start_logging();
    let predicate = PredicateBuilder::default()
        .timestamp_range(MIN_NANO_TIME, MAX_NANO_TIME - 1) // one less than max timestamp
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec![];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

#[tokio::test]
async fn list_tag_name_max_time_greater_one() {
    test_helpers::maybe_start_logging();
    let predicate = PredicateBuilder::default()
        .timestamp_range(MIN_NANO_TIME + 1, MAX_NANO_TIME) // one more than min timestamp
        .build();
    let predicate = InfluxRpcPredicate::new(None, predicate);
    let expected_tag_keys = vec![];
    run_tag_keys_test_case(MeasurementWithMaxTime {}, predicate, expected_tag_keys).await;
}

fn to_stringset(v: &[&str]) -> StringSetRef {
    v.into_stringset().unwrap()
}
