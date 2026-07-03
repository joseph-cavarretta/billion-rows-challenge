//! Benchmark: Rust + DataFusion querying a Vortex file.
//!
//! STUB — the harness (arg parsing, async runtime) is done; implement `run`.
//!
//! This is the headline entry for Part 2: a systems language reading Vortex
//! through DataFusion. Vortex ships a DataFusion `TableProvider`, so once the
//! file is registered you can drive the aggregation with either SQL or the
//! DataFrame API.

use std::path::PathBuf;
use std::process::ExitCode;

#[tokio::main]
async fn main() -> ExitCode {
    let path = match std::env::args().nth(1) {
        Some(p) => PathBuf::from(p),
        None => {
            eprintln!("usage: vortex-bench <path/to/measurements.vortex>");
            return ExitCode::FAILURE;
        }
    };
    if !path.is_file() {
        eprintln!("No input file present at {}", path.display());
        return ExitCode::FAILURE;
    }

    match run(&path).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("error: {e}");
            ExitCode::FAILURE
        }
    }
}

/// Aggregate min/mean/max reading per station from a Vortex file.
///
/// TODO(you): implement this.
///   1. build a DataFusion `SessionContext`
///   2. open the Vortex file and register it as a table via the Vortex
///      DataFusion `TableProvider` (see the vortex-datafusion crate docs)
///   3. run the aggregation, e.g.:
///        SELECT station, avg(reading), min(reading), max(reading)
///        FROM measurements GROUP BY station ORDER BY station
///      using ctx.sql(...) or the DataFrame API
///   4. collect the batches and print them (df.show().await? is easiest)
///
/// Stretch goal: compare a full scan vs. a filtered query — Vortex can evaluate
/// predicates on compressed segments without fully decompressing them.
async fn run(_path: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    todo!("Implement the Rust + DataFusion + Vortex benchmark")
}
