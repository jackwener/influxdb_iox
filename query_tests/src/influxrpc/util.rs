use query::exec::IOxExecutionContext;
use query::{exec::seriesset::SeriesSetItem, plan::seriesset::SeriesSetPlans};

/// Run a series set plan to completion and produce a Vec<String> representation
///
/// # Panics
///
/// Panics if there is an error executing a plan, or if unexpected series set
/// items are returned.
#[cfg(test)]
pub async fn run_series_set_plan(ctx: &IOxExecutionContext, plans: SeriesSetPlans) -> Vec<String> {
    let results = ctx.to_series_set(plans).await;

    let mut results = results
        .unwrap()
        .into_iter()
        .map(|item| {
            if let SeriesSetItem::Data(series_set) = item {
                series_set
            } else {
                panic!(
                    "Unexpected result from converting. Expected SeriesSetItem::Data, got: {:?}",
                    item
                )
            }
        })
        .collect::<Vec<_>>();

    // sort the results so that we can reliably compare
    results.sort_by(|r1, r2| {
        r1.table_name
            .cmp(&r2.table_name)
            .then(r1.tags.cmp(&r2.tags))
    });

    results
        .into_iter()
        .map(|s| {
            s.to_string()
                .trim()
                .split('\n')
                .into_iter()
                .map(|s| s.to_string())
                .collect::<Vec<_>>()
        })
        .flatten()
        .collect::<Vec<_>>()
}
