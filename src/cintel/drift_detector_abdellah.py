"""
drift_detector_abdellah.py - Custom drift detection project script.

Author: Abdellah Boudlal
Date: 2026-04

Project Overview
----------------
This script compares reference system metrics with current system metrics
to detect possible drift in system behavior.

It extends the example project by adding:
1. Percentage change columns for each metric
2. An overall drift flag that summarizes whether any major metric is drifting

Input Files
-----------
- data/reference_metrics_case.csv
- data/current_metrics_case.csv

Output Files
------------
- artifacts/drift_summary_abdellah.csv
- artifacts/drift_summary_long_abdellah.csv

How to Run
----------
Run this file from the root project folder:

    uv run python -m cintel.drift_detector_abdellah

Purpose
-------
- Read reference and current system metrics from CSV files
- Compare the two datasets using summary statistics
- Detect meaningful changes in average system behavior
- Add percentage-based comparisons for easier interpretation
- Add an overall drift flag for quick summary
- Save output artifacts for review
- Log the pipeline process for transparency and debugging
"""

# ============================================================
# IMPORTS
# ============================================================

import logging
from pathlib import Path
from typing import Final

import polars as pl
from datafun_toolkit.logger import get_logger, log_header, log_path

# ============================================================
# LOGGER CONFIGURATION
# ============================================================

LOG: logging.Logger = get_logger("P5", level="DEBUG")

# ============================================================
# GLOBAL PATHS
# ============================================================

ROOT_DIR: Final[Path] = Path.cwd()
DATA_DIR: Final[Path] = ROOT_DIR / "data"
ARTIFACTS_DIR: Final[Path] = ROOT_DIR / "artifacts"

REFERENCE_FILE: Final[Path] = DATA_DIR / "reference_metrics_case.csv"
CURRENT_FILE: Final[Path] = DATA_DIR / "current_metrics_case.csv"

OUTPUT_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_abdellah.csv"
SUMMARY_LONG_FILE: Final[Path] = ARTIFACTS_DIR / "drift_summary_long_abdellah.csv"

# ============================================================
# DRIFT THRESHOLDS
# ============================================================

# These thresholds define how large a difference must be
# before we consider it a meaningful sign of drift.

REQUESTS_DRIFT_THRESHOLD: Final[float] = 20.0
ERRORS_DRIFT_THRESHOLD: Final[float] = 2.0
LATENCY_DRIFT_THRESHOLD: Final[float] = 1000.0


# ============================================================
# HELPER FUNCTION
# ============================================================


def safe_percent_change(
    current_col: str, reference_col: str, alias_name: str
) -> pl.Expr:
    """
    Create a percentage change expression safely.

    Formula:
        ((current - reference) / reference) * 100

    If the reference value is 0, the result is set to null
    to avoid division by zero.

    Args:
        current_col: Name of the current-period column
        reference_col: Name of the reference-period column
        alias_name: Name for the new output column

    Returns:
        A Polars expression for percentage change.
    """
    return (
        pl.when(pl.col(reference_col) != 0)
        .then(
            ((pl.col(current_col) - pl.col(reference_col)) / pl.col(reference_col))
            * 100
        )
        .otherwise(None)
        .round(2)
        .alias(alias_name)
    )


# ============================================================
# MAIN PIPELINE
# ============================================================


def main() -> None:
    """
    Run the custom drift detection pipeline.

    Steps:
    1. Read reference and current CSV files
    2. Calculate average metrics for each period
    3. Combine the summaries into one row
    4. Calculate difference columns
    5. Calculate percentage change columns
    6. Create drift flags for each metric
    7. Create an overall drift flag
    8. Save the flat summary artifact
    9. Save a long-form display artifact
    10. Log the process
    """
    log_header(LOG, "CINTEL")

    LOG.info("========================")
    LOG.info("START main()")
    LOG.info("========================")

    # Log key paths
    log_path(LOG, "ROOT_DIR", ROOT_DIR)
    log_path(LOG, "REFERENCE_FILE", REFERENCE_FILE)
    log_path(LOG, "CURRENT_FILE", CURRENT_FILE)
    log_path(LOG, "OUTPUT_FILE", OUTPUT_FILE)
    log_path(LOG, "SUMMARY_LONG_FILE", SUMMARY_LONG_FILE)

    # Ensure artifacts directory exists
    ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
    log_path(LOG, "ARTIFACTS_DIR", ARTIFACTS_DIR)

    # --------------------------------------------------------
    # STEP 1: READ INPUT FILES
    # --------------------------------------------------------
    reference_df = pl.read_csv(REFERENCE_FILE)
    current_df = pl.read_csv(CURRENT_FILE)

    LOG.info(f"Loaded {reference_df.height} reference records")
    LOG.info(f"Loaded {current_df.height} current records")

    # --------------------------------------------------------
    # STEP 2: CALCULATE AVERAGE METRICS FOR EACH PERIOD
    # --------------------------------------------------------
    reference_summary_df = reference_df.select(
        [
            pl.col("requests").mean().alias("reference_avg_requests"),
            pl.col("errors").mean().alias("reference_avg_errors"),
            pl.col("total_latency_ms").mean().alias("reference_avg_latency_ms"),
        ]
    )

    current_summary_df = current_df.select(
        [
            pl.col("requests").mean().alias("current_avg_requests"),
            pl.col("errors").mean().alias("current_avg_errors"),
            pl.col("total_latency_ms").mean().alias("current_avg_latency_ms"),
        ]
    )

    LOG.info("Calculated reference and current summary averages")

    # --------------------------------------------------------
    # STEP 3: COMBINE REFERENCE AND CURRENT SUMMARIES
    # --------------------------------------------------------
    combined_df: pl.DataFrame = pl.concat(
        [reference_summary_df, current_summary_df],
        how="horizontal",
    )

    # --------------------------------------------------------
    # STEP 4: CALCULATE DIFFERENCE COLUMNS
    # --------------------------------------------------------
    requests_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_requests") - pl.col("reference_avg_requests"))
        .round(2)
        .alias("requests_mean_difference")
    )

    errors_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_errors") - pl.col("reference_avg_errors"))
        .round(2)
        .alias("errors_mean_difference")
    )

    latency_mean_difference_recipe: pl.Expr = (
        (pl.col("current_avg_latency_ms") - pl.col("reference_avg_latency_ms"))
        .round(2)
        .alias("latency_mean_difference_ms")
    )

    drift_df: pl.DataFrame = combined_df.with_columns(
        [
            requests_mean_difference_recipe,
            errors_mean_difference_recipe,
            latency_mean_difference_recipe,
        ]
    )

    # --------------------------------------------------------
    # STEP 5: CALCULATE PERCENT CHANGE COLUMNS
    # --------------------------------------------------------
    # New modification:
    # These columns make the results easier to interpret by
    # showing relative change, not only absolute difference.
    drift_df = drift_df.with_columns(
        [
            safe_percent_change(
                "current_avg_requests",
                "reference_avg_requests",
                "requests_percent_change",
            ),
            safe_percent_change(
                "current_avg_errors",
                "reference_avg_errors",
                "errors_percent_change",
            ),
            safe_percent_change(
                "current_avg_latency_ms",
                "reference_avg_latency_ms",
                "latency_percent_change",
            ),
        ]
    )

    # --------------------------------------------------------
    # STEP 6: CREATE DRIFT FLAGS FOR EACH METRIC
    # --------------------------------------------------------
    requests_is_drifting_flag_recipe: pl.Expr = (
        pl.col("requests_mean_difference").abs() > REQUESTS_DRIFT_THRESHOLD
    ).alias("requests_is_drifting_flag")

    errors_is_drifting_flag_recipe: pl.Expr = (
        pl.col("errors_mean_difference").abs() > ERRORS_DRIFT_THRESHOLD
    ).alias("errors_is_drifting_flag")

    latency_is_drifting_flag_recipe: pl.Expr = (
        pl.col("latency_mean_difference_ms").abs() > LATENCY_DRIFT_THRESHOLD
    ).alias("latency_is_drifting_flag")

    drift_df = drift_df.with_columns(
        [
            requests_is_drifting_flag_recipe,
            errors_is_drifting_flag_recipe,
            latency_is_drifting_flag_recipe,
        ]
    )

    # --------------------------------------------------------
    # STEP 7: CREATE AN OVERALL DRIFT FLAG
    # --------------------------------------------------------
    # New modification:
    # This flag becomes True if any of the individual drift
    # flags are True. It gives a quick summary for the analyst.
    overall_drift_flag_recipe: pl.Expr = (
        pl.col("requests_is_drifting_flag")
        | pl.col("errors_is_drifting_flag")
        | pl.col("latency_is_drifting_flag")
    ).alias("overall_drift_flag")

    drift_df = drift_df.with_columns([overall_drift_flag_recipe])

    LOG.info("Calculated differences, percent changes, and drift flags")

    # --------------------------------------------------------
    # STEP 8: SAVE THE FLAT SUMMARY ARTIFACT
    # --------------------------------------------------------
    drift_df.write_csv(OUTPUT_FILE)
    LOG.info(f"Wrote drift summary file: {OUTPUT_FILE}")

    LOG.info("Drift summary dataframe:")
    LOG.info(drift_df)

    # --------------------------------------------------------
    # STEP 9: LOG ONE FIELD PER LINE
    # --------------------------------------------------------
    drift_summary_dict = drift_df.to_dicts()[0]

    LOG.info("========================")
    LOG.info("Drift Detection Process")
    LOG.info("========================")
    LOG.info("1. Summarize each period with means.")
    LOG.info("2. Compute differences between current and reference values.")
    LOG.info("3. Compute percent changes for easier interpretation.")
    LOG.info("4. Flag drift if differences exceed thresholds.")
    LOG.info("5. Create an overall drift summary flag.")
    LOG.info("========================")

    LOG.info("Drift summary (one field per line):")
    for field_name, field_value in drift_summary_dict.items():
        LOG.info(f"{field_name}: {field_value}")

    # --------------------------------------------------------
    # STEP 10: CREATE LONG-FORM ARTIFACT
    # --------------------------------------------------------
    drift_summary_long_df = pl.DataFrame(
        {
            "field_name": list(drift_summary_dict.keys()),
            "field_value": [str(value) for value in drift_summary_dict.values()],
        }
    )

    drift_summary_long_df.write_csv(SUMMARY_LONG_FILE)
    LOG.info(f"Wrote long summary file: {SUMMARY_LONG_FILE}")

    LOG.info("========================")
    LOG.info("Pipeline executed successfully!")
    LOG.info("========================")
    LOG.info("END main()")


# ============================================================
# CONDITIONAL EXECUTION
# ============================================================

if __name__ == "__main__":
    main()
