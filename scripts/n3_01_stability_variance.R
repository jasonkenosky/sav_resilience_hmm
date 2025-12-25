# ============================================================
# Script:      n3_01_stability_variance.R
# Purpose:     Quantify stability / volatility of lineage state trajectories
# Author:      Jason Kenosky
# Stage:       N3 – Stability
#
# Definition:
#   Stability is operationalized as *low temporal volatility*.
#   For categorical states, volatility is measured via switching behavior
#   rather than numeric variance.
#
# Inputs:
#   outputs/data/n0_02d_build_lineage_timeseries_table_long_latest.csv
#
# Outputs:
#   outputs/metrics/n3_01_stability_variance_metrics_latest.csv
#   outputs/metrics/n3_01_stability_variance_metrics_<timestamp>.csv
#   outputs/qc/n3_01_stability_variance_qc_latest.csv
#   outputs/qc/n3_01_stability_variance_qc_<timestamp>.csv
# ============================================================

# ------------------------------------------------
# 0. Housekeeping 
# ------------------------------------------------
rm(list = ls())
gc()

options(
  scipen = 999,
  dplyr.summarise.inform = FALSE
)

# ------------------------------------------------
# 1. Load packages
# ------------------------------------------------

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glue)
  library(here)
  library(fs)
  library(janitor)
})

# ------------------------------------------------------------
# 2. Script configuration
# ------------------------------------------------------------

SCRIPT_ID <- "n3_01_stability_variance"

DIR_ROOT   <- here::here()
DIR_DATA   <- file.path(DIR_ROOT, "outputs", "data")
DIR_METRIC <- file.path(DIR_ROOT, "outputs", "metrics")
DIR_QC     <- file.path(DIR_ROOT, "outputs", "qc")
DIR_LOGS   <- file.path(DIR_ROOT, "outputs", "logs")

fs::dir_create(c(DIR_METRIC, DIR_QC, DIR_LOGS), recurse = TRUE)

FILE_IN <- file.path(
  DIR_DATA,
  "n0_02d_build_lineage_timeseries_table_long_latest.csv"
)

timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

FILE_OUT_LATEST <- file.path(
  DIR_METRIC,
  "n3_01_stability_variance_metrics_latest.csv"
)

FILE_OUT_RUN <- file.path(
  DIR_METRIC,
  glue("n3_01_stability_variance_metrics_{timestamp}.csv")
)

FILE_QC_LATEST <- file.path(
  DIR_QC,
  "n3_01_stability_variance_qc_latest.csv"
)

FILE_QC_RUN <- file.path(
  DIR_QC,
  glue("n3_01_stability_variance_qc_{timestamp}.csv")
)

PARAMS <- list(
  required_cols = c("lineage_id", "year", "state_bin7")
)

# ------------------------------------------------------------
#  3. Logging
# ------------------------------------------------------------

source(here::here("R", "logger_helpers.R"))
log_meta <- start_log(SCRIPT_ID, DIR_LOGS)
on.exit(stop_log(log_meta$log_con), add = TRUE)

log_system_info()
log_packages()

log_section("N3 – STABILITY (VARIANCE)")

log_inputs_outputs(
  inputs = c("Stage0 long table" = FILE_IN),
  outputs = c(
    "Metrics latest" = FILE_OUT_LATEST,
    "Metrics run"    = FILE_OUT_RUN,
    "QC latest"      = FILE_QC_LATEST,
    "QC run"         = FILE_QC_RUN
  )
)

log_params(PARAMS)

# ------------------------------------------------------------
# 4. Inputs
# ------------------------------------------------------------

dat <- with_timing(
  "Read long table",
  readr::read_csv(FILE_IN, show_col_types = FALSE)
)

dat <- janitor::clean_names(dat)

missing <- setdiff(PARAMS$required_cols, names(dat))
if (length(missing) > 0) {
  log_error(glue("Missing required columns: {paste(missing, collapse = ', ')}"))
  stop("Input validation failed")
}

log_info(glue(
  "Rows: {nrow(dat)} | Lineages: {n_distinct(dat$lineage_id)} | Years: {n_distinct(dat$year)}"
))

# ------------------------------------------------
# 5. Helpers (script-local only)
# ------------------------------------------------

# ------------------------------------------------------------
# 6. Main process: Compute stability metrics
# ------------------------------------------------------------

log_section("COMPUTE STABILITY METRICS")

metrics <- with_timing(
  "Per-lineage switching / volatility",
  dat %>%
    arrange(lineage_id, year) %>%
    group_by(lineage_id) %>%
    mutate(
      prev_state = dplyr::lag(state_bin7),
      switched   = !is.na(prev_state) & state_bin7 != prev_state
    ) %>%
    summarise(
      n_obs_years       = n(),
      n_transitions     = max(n_obs_years - 1, 0),
      n_state_changes   = sum(switched, na.rm = TRUE),
      switching_rate    = ifelse(n_transitions > 0,
                                 n_state_changes / n_transitions,
                                 NA_real_),
      longest_same_state_run =
        {
          r <- rle(state_bin7)
          max(r$lengths, na.rm = TRUE)
        },
      .groups = "drop"
    )
)

log_info(glue("Computed metrics for {nrow(metrics)} lineages"))

# ------------------------------------------------------------
# 7. Write outputs
# ------------------------------------------------------------

log_section("WRITE OUTPUTS")

readr::write_csv(metrics, FILE_OUT_LATEST)
readr::write_csv(metrics, FILE_OUT_RUN)

qc <- tibble(
  timestamp        = Sys.time(),
  script_id        = SCRIPT_ID,
  n_lineages       = nrow(metrics),
  median_switching = median(metrics$switching_rate, na.rm = TRUE),
  p90_switching    = quantile(metrics$switching_rate, 0.9, na.rm = TRUE)
)

readr::write_csv(qc, FILE_QC_LATEST)
readr::write_csv(qc, FILE_QC_RUN)

# ------------------------------------------------
# 8. QC summary (always write)
# ------------------------------------------------

# ------------------------------------------------
# 9. Done
# ------------------------------------------------

log_section("SCRIPT COMPLETE")
log_info("Stability / variance metrics completed successfully.")
