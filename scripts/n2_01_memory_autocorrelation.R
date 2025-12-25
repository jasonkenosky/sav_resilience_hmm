# ==============================================================================
# Script Title: n2_01_memory_autocorrelation
# Purpose: Quantify "memory" of observed SAV state trajectories per lineage
# Author: Jason Kenosky
# Last Updated: 2025-12-16
#
# Description:
#   - Reads canonical Stage 0 long table (lineage_id × year × state_bin7)
#   - Computes run-length structure + lag-1 self-dependence per lineage
#   - Treats missing years as run breaks (no interpolation)
#   - Writes one metrics table + QC summary
#
# Inputs:
#   outputs/data/n0_02d_build_lineage_timeseries_table_long_latest.csv
#
# Outputs:
#   outputs/metrics/n2_01_memory_autocorrelation_metrics_latest.csv
#   outputs/metrics/n2_01_memory_autocorrelation_metrics_<timestamp>.csv
#   outputs/qc/n2_01_memory_autocorrelation_qc_latest.csv
#   outputs/qc/n2_01_memory_autocorrelation_qc_<timestamp>.csv
#   outputs/logs/n2_01_memory_autocorrelation_<timestamp>.log
# ==============================================================================

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
  library(fs)
  library(here)
  library(janitor)
  library(tibble)
})

# ------------------------------------------------
# 2. Script configuration
# ------------------------------------------------
SCRIPT_ID <- "n2_01_memory_autocorrelation"

DIR_ROOT    <- here::here()
DIR_OUTPUTS <- file.path(DIR_ROOT, "outputs")
DIR_DATA    <- file.path(DIR_OUTPUTS, "data")
DIR_LOGS    <- file.path(DIR_OUTPUTS, "logs")
DIR_QC      <- file.path(DIR_OUTPUTS, "qc")
DIR_METRICS <- file.path(DIR_OUTPUTS, "metrics")

fs::dir_create(DIR_OUTPUTS, recurse = TRUE)
fs::dir_create(DIR_DATA, recurse = TRUE)
fs::dir_create(DIR_LOGS, recurse = TRUE)
fs::dir_create(DIR_QC, recurse = TRUE)
fs::dir_create(DIR_METRICS, recurse = TRUE)

FILE_IN_LONG <- file.path(DIR_DATA, "n0_02d_build_lineage_timeseries_table_long_latest.csv")

timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

FILE_OUT_LATEST <- file.path(DIR_METRICS, glue("{SCRIPT_ID}_metrics_latest.csv"))
FILE_OUT_RUN    <- file.path(DIR_METRICS, glue("{SCRIPT_ID}_metrics_{timestamp}.csv"))

FILE_QC_LATEST  <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_latest.csv"))
FILE_QC_RUN     <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_{timestamp}.csv"))

PARAMS <- list(
  required_cols = c("lineage_id", "year", "state_bin7"),
  # missing-year behavior: break runs; lag-1 only when years are consecutive
  require_consecutive_years_for_lag1 = TRUE
)

# ------------------------------------------------
# 3. Logging setup (your helpers)
# ------------------------------------------------
FILE_LOG_HELPERS <- here::here("R", "logger_helpers.R")
if (!file.exists(FILE_LOG_HELPERS)) stop("Missing logger helpers: ", FILE_LOG_HELPERS)
source(FILE_LOG_HELPERS)

log_meta <- start_log(SCRIPT_ID, DIR_LOGS)
on.exit(stop_log(log_meta$log_con), add = TRUE)

log_system_info()
log_packages()

log_section(glue("{SCRIPT_ID} - START"))
log_inputs_outputs(
  inputs = list("Stage0 long table" = FILE_IN_LONG),
  outputs = list(
    "Metrics latest" = FILE_OUT_LATEST,
    "Metrics run"    = FILE_OUT_RUN,
    "QC latest"      = FILE_QC_LATEST,
    "QC run"         = FILE_QC_RUN
  )
)
log_params(list(
  required_cols = paste(PARAMS$required_cols, collapse = ", "),
  require_consecutive_years_for_lag1 = PARAMS$require_consecutive_years_for_lag1
))

# ------------------------------------------------
# 4. Helpers (script-local)
# ------------------------------------------------
safe_write_csv <- function(df, path) {
  fs::dir_create(dirname(path), recurse = TRUE)
  readr::write_csv(df, path)
  invisible(path)
}

qc_row <- function(metric, value, notes = NA_character_) {
  tibble::tibble(
    timestamp = Sys.time(),
    script_id = SCRIPT_ID,
    metric    = metric,
    value     = as.character(value),
    notes     = notes
  )
}

write_qc <- function(qc_tbl, file_latest, file_run) {
  safe_write_csv(qc_tbl, file_latest)
  safe_write_csv(qc_tbl, file_run)
  invisible(TRUE)
}

check_required_cols <- function(df, required_cols, context = "data") {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    msg <- glue("Missing required columns in {context}: {paste(missing, collapse = ', ')}")
    log_error(msg)
    stop(msg)
  }
  invisible(TRUE)
}

assert_unique_key <- function(df, key = c("lineage_id", "year"), context = "data") {
  dup_n <- df %>%
    dplyr::count(dplyr::across(dplyr::all_of(key))) %>%
    dplyr::filter(n > 1) %>%
    nrow()
  if (dup_n > 0) {
    msg <- glue("Non-unique key in {context}: duplicates found for ({paste(key, collapse = ', ')}) | n_dup_keys={dup_n}")
    log_warn(msg)
    # do not stop; Stage 0 may occasionally have duplicates; we proceed conservatively
  }
  invisible(TRUE)
}

# Run-lengths per lineage, treating missing years as breaks.
# Returns a tibble of run lengths (one row per run).
compute_runs <- function(df_one) {
  df_one <- df_one %>%
    dplyr::arrange(year) %>%
    dplyr::mutate(
      year_lag = dplyr::lag(year),
      state_lag = dplyr::lag(state_bin7),
      is_consecutive = (year == year_lag + 1L),
      # new run starts if first row, OR year gap, OR state changes
      new_run = dplyr::if_else(
        is.na(year_lag) | !is_consecutive | (state_bin7 != state_lag),
        TRUE, FALSE
      ),
      run_id = cumsum(dplyr::coalesce(new_run, TRUE))
    )
  
  df_one %>%
    dplyr::group_by(run_id) %>%
    dplyr::summarise(
      run_len = dplyr::n(),
      run_state = dplyr::first(state_bin7),
      year_start = dplyr::first(year),
      year_end   = dplyr::last(year),
      .groups = "drop"
    )
}

# Lag-1 same-state probability. Only computed on consecutive years if requested.
compute_lag1_same <- function(df_one, require_consecutive = TRUE) {
  df_one <- df_one %>%
    dplyr::arrange(year) %>%
    dplyr::mutate(
      state_lag = dplyr::lag(state_bin7),
      year_lag  = dplyr::lag(year),
      is_consecutive = (year == year_lag + 1L),
      eligible = !is.na(state_lag) & (!require_consecutive | is_consecutive),
      same = eligible & (state_bin7 == state_lag)
    )
  
  denom <- sum(df_one$eligible, na.rm = TRUE)
  if (denom == 0) return(NA_real_)
  sum(df_one$same, na.rm = TRUE) / denom
}

# ------------------------------------------------
# 5. Read input + validate
# ------------------------------------------------
log_section("READ + VALIDATE INPUT")

if (!file.exists(FILE_IN_LONG)) {
  log_error(glue("Missing input file: {FILE_IN_LONG}"))
  stop("Missing input file: ", FILE_IN_LONG)
}

dat <- with_timing("Read long table", quote({
  readr::read_csv(FILE_IN_LONG, show_col_types = FALSE) |>
    janitor::clean_names()
}))

check_required_cols(dat, PARAMS$required_cols, context = "Stage 0 long table")
assert_unique_key(dat, key = c("lineage_id", "year"), context = "Stage 0 long table")

# enforce types
dat <- dat %>%
  dplyr::mutate(
    lineage_id = suppressWarnings(as.integer(lineage_id)),
    year       = suppressWarnings(as.integer(year)),
    state_bin7 = as.character(state_bin7)
  )

# hard stops
stopifnot(!any(is.na(dat$lineage_id)))
stopifnot(!any(is.na(dat$year)))
stopifnot(!any(is.na(dat$state_bin7)))

log_info(glue("Rows: {nrow(dat)} | Lineages: {dplyr::n_distinct(dat$lineage_id)} | Years: {dplyr::n_distinct(dat$year)}"))
log_info(glue("Year span: {min(dat$year)}-{max(dat$year)}"))

# ------------------------------------------------
# 6. Compute memory metrics
# ------------------------------------------------
log_section("COMPUTE MEMORY METRICS")

metrics <- with_timing("Per-lineage runs + lag-1 dependence", quote({
  dat %>%
    dplyr::arrange(lineage_id, year) %>%
    dplyr::group_by(lineage_id) %>%
    dplyr::summarise(
      n_obs_years = dplyr::n(),
      year_min    = min(year),
      year_max    = max(year),
      span_years  = year_max - year_min + 1L,
      p_same_lag1 = compute_lag1_same(
        dplyr::pick(year, state_bin7),
        require_consecutive = PARAMS$require_consecutive_years_for_lag1
      ),
      # run stats computed via helper on the grouped data (no lineage_id needed inside)
      n_runs            = nrow(compute_runs(dplyr::pick(year, state_bin7))),
      median_run_length = stats::median(compute_runs(dplyr::pick(year, state_bin7))$run_len),
      mean_run_length   = mean(compute_runs(dplyr::pick(year, state_bin7))$run_len),
      max_run_length    = max(compute_runs(dplyr::pick(year, state_bin7))$run_len),
      .groups = "drop"
    )
}))

log_info(glue("Metrics rows: {nrow(metrics)} (should equal n_lineages)"))

# ------------------------------------------------
# 7. QC summary
# ------------------------------------------------
log_section("QC SUMMARY")

qc <- tibble::tibble()

qc <- dplyr::bind_rows(
  qc,
  qc_row("input_rows", nrow(dat)),
  qc_row("input_lineages", dplyr::n_distinct(dat$lineage_id)),
  qc_row("input_year_min", min(dat$year)),
  qc_row("input_year_max", max(dat$year)),
  qc_row("metrics_rows", nrow(metrics)),
  qc_row("metrics_lineages", dplyr::n_distinct(metrics$lineage_id)),
  qc_row("p_same_lag1_na", sum(is.na(metrics$p_same_lag1))),
  qc_row("median_run_length_median", stats::median(metrics$median_run_length, na.rm = TRUE)),
  qc_row("max_run_length_max", max(metrics$max_run_length, na.rm = TRUE))
)

# ------------------------------------------------
# 8. Write outputs
# ------------------------------------------------
log_section("WRITE OUTPUTS")

with_timing("Write metrics + QC", quote({
  safe_write_csv(metrics, FILE_OUT_LATEST)
  safe_write_csv(metrics, FILE_OUT_RUN)
  write_qc(qc, FILE_QC_LATEST, FILE_QC_RUN)
}))

log_info(glue("Metrics latest: {FILE_OUT_LATEST}"))
log_info(glue("Metrics run:    {FILE_OUT_RUN}"))
log_info(glue("QC latest:      {FILE_QC_LATEST}"))
log_info(glue("QC run:         {FILE_QC_RUN}"))

# ------------------------------------------------
# 9. Done
# ------------------------------------------------
log_section("DONE")
log_info("N2 memory metrics completed successfully.")

