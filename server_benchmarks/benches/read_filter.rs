use criterion::{BenchmarkId, Criterion};
use datafusion::logical_plan::{col, lit};
use std::io::Read;
// This is a struct that tells Criterion.rs to use the "futures" crate's
// current-thread executor
use db::Db;
use flate2::read::GzDecoder;
use predicate::rpc_predicate::InfluxRpcPredicate;
use predicate::PredicateBuilder;
use query::{
    exec::{Executor, ExecutorType},
    frontend::influxrpc::InfluxRpcPlanner,
};
use query_tests::scenarios::DbScenario;
use tokio::runtime::Runtime;

// Uses the `query_tests` module to generate some chunk scenarios, specifically
// the scenarios where there are:
//
// - a single open mutable buffer chunk;
// - a closed mutable buffer chunk and another open one;
// - an open mutable buffer chunk and a closed read buffer chunk;
// - two closed read buffer chunks.
//
// The chunks are all fed the *same* line protocol, so these benchmarks are
// useful for assessing the differences in performance between querying the
// chunks held in different execution engines.
//
// These benchmarks use a synthetically generated set of line protocol using
// `inch`. Each point is a new series containing 10 tag keys, which results in
// ten columns in IOx. There is a single field column and a timestamp column.
//
//   - tag0, cardinality 2.
//   - tag1, cardinality 10.
//   - tag2, cardinality 10.
//   - tag3, cardinality 50.
//   - tag4, cardinality 100.
//
// In total there are 10K rows. The timespan of the points in the line
// protocol is around 1m of wall-clock time.
async fn setup_scenarios() -> Vec<DbScenario> {
    let raw = include_bytes!("../../test_fixtures/lineproto/read_filter.lp.gz");
    let mut gz = GzDecoder::new(&raw[..]);
    let mut lp = String::new();
    gz.read_to_string(&mut lp).unwrap();

    let db =
        query_tests::scenarios::util::make_two_chunk_scenarios("2021-04-26T13", &lp, &lp).await;
    db
}

// Run all benchmarks for `read_filter`.
pub fn benchmark_read_filter(c: &mut Criterion) {
    let scenarios = Runtime::new().unwrap().block_on(setup_scenarios());
    execute_benchmark_group(c, scenarios.as_slice());
}

// Runs an async criterion benchmark against the provided scenarios and
// predicate.
fn execute_benchmark_group(c: &mut Criterion, scenarios: &[DbScenario]) {
    let planner = InfluxRpcPlanner::new();

    let predicates = vec![
        (InfluxRpcPredicate::default(), "no_pred"),
        (
            InfluxRpcPredicate::new(
                None,
                PredicateBuilder::default()
                    .add_expr(col("tag3").eq(lit("value49")))
                    .build(),
            ),
            "with_pred_tag_3=value49",
        ),
    ];

    for scenario in scenarios {
        let DbScenario { scenario_name, db } = scenario;
        let mut group = c.benchmark_group(format!("read_filter/{}", scenario_name));

        for (predicate, pred_name) in &predicates {
            let chunks = db
                .filtered_chunk_summaries(None, Some("2021-04-26T13"))
                .len();
            // The number of expected frames, based on the expected number of
            // individual series keys.
            let exp_data_frames = if predicate.is_empty() { 10000 } else { 200 } * chunks;

            group.bench_with_input(
                BenchmarkId::from_parameter(pred_name),
                predicate,
                |b, predicate| {
                    let executor = db.executor();
                    b.to_async(Runtime::new().unwrap()).iter(|| {
                        build_and_execute_plan(
                            &planner,
                            executor.as_ref(),
                            db,
                            predicate.clone(),
                            exp_data_frames,
                        )
                    });
                },
            );
        }

        group.finish();
    }
}

// Plans and runs a tag_values query.
async fn build_and_execute_plan(
    planner: &InfluxRpcPlanner,
    executor: &Executor,
    db: &Db,
    predicate: InfluxRpcPredicate,
    exp_data_frames: usize,
) {
    let plan = planner
        .read_filter(db, predicate)
        .expect("built plan successfully");

    let results = executor
        .new_context(ExecutorType::Query)
        .to_series_and_groups(plan)
        .await
        .expect("Running series set plan");

    assert_eq!(results.len(), exp_data_frames);
}
