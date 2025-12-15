# ==============================================================================
# Script Title: n0_01_validate_inputs
# Purpose: Validate that canonical SAV lineage inputs exist and contain required
#          columns and expected time/state coverage before any analysis begins.
# Author: Jason Kenosky
# Last Updated: 2025-12-15
#
# Description:
#   - Confirms the canonical lineage inputs (GPKG and/or CSV) exist
#   - Validates required columns for HMM resilience workflow
#   - Reports key counts and coverage (years, lineages, states, CBPSEG, QUADID)
#   - Writes QC summary tables and a log for auditability
#
# Inputs:
#   data/data_processed/sav_lineages_hmm.gpkg
#   data/data_processed/sav_lineages_hmm.csv
#
# Outputs:
#   outputs/qc/n0_01_validate_inputs_qc_latest.csv
#   outputs/qc/n0_01_validate_inputs_qc_<timestamp>.csv
#   outputs/logs/n0_01_validate_inputs_<timestamp>.log
#
# Notes:
#   - Package versions and session info are logged
#   - This script performs no data transformations and writes no processed data
# ==============================================================================

# ------------------------------------------------
# 0. Housekeeping (clean session friendly)
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
  library(fs)
  library(here)
  library(janitor)
  library(tibble)
  library(sf)         # needed for GPKG validation
})

# ------------------------------------------------
# 2. Script configuration (directories, parameters)
# ------------------------------------------------
SCRIPT_ID <- "n0_01_validate_inputs"

DIR_ROOT <- here::here()

# Standard project dirs
DIR_DATA_RAW       <- file.path(DIR_ROOT, "data", "data_raw")
DIR_DATA_PROCESSED <- file.path(DIR_ROOT, "data", "data_processed")
DIR_SCRIPTS        <- file.path(DIR_ROOT, "scripts")
DIR_R              <- file.path(DIR_ROOT, "R")
DIR_OUTPUTS        <- file.path(DIR_ROOT, "outputs")
DIR_LOGS           <- file.path(DIR_OUTPUTS, "logs")
DIR_QC             <- file.path(DIR_OUTPUTS, "qc")

# Ensure core dirs exist
fs::dir_create(DIR_DATA_PROCESSED, recurse = TRUE)
fs::dir_create(DIR_OUTPUTS, recurse = TRUE)
fs::dir_create(DIR_LOGS, recurse = TRUE)
fs::dir_create(DIR_QC, recurse = TRUE)

# Canonical expected inputs (you can change filenames here ONLY if the project decides so)
FILE_IN_GPKG <- file.path(DIR_DATA_PROCESSED, "sav_lineages_hmm.gpkg")
FILE_IN_CSV  <- file.path(DIR_DATA_PROCESSED, "sav_lineages_hmm.csv")

# QC files
FILE_QC_LATEST <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_latest.csv"))

# Parameters (log these up front; keep boring and explicit)
PARAMS <- list(
  required_cols = c("lineage_id", "year", "hmm_state", "cbpseg", "quadid"),
  gpkg_path     = FILE_IN_GPKG,
  csv_path      = FILE_IN_CSV
)

# ------------------------------------------------
# 3. Logging setup (your helpers only)
# ------------------------------------------------
FILE_LOG_HELPERS <- here::here("R", "logger_helpers.R")
if (!file.exists(FILE_LOG_HELPERS)) {
  stop("Missing logger helpers: ", FILE_LOG_HELPERS)
}
source(FILE_LOG_HELPERS)

log_meta <- start_log(SCRIPT_ID, DIR_LOGS)
on.exit(stop_log(log_meta$log_con), add = TRUE)

log_section(glue("{SCRIPT_ID} - START"))
log_info(glue("Project root: {DIR_ROOT}"))
log_info(glue("Working dir:  {getwd()}"))
log_info(glue("Log file:     {log_meta$log_file}"))

log_section("PARAMETERS")
log_info(glue("Required columns: {paste(PARAMS$required_cols, collapse = ', ')}"))
log_info(glue("Expected GPKG:    {PARAMS$gpkg_path}"))
log_info(glue("Expected CSV:     {PARAMS$csv_path}"))

# ------------------------------------------------
# 4. Inputs (validate early)
# ------------------------------------------------
log_section("INPUTS")

# Require at least one of the canonical inputs to exist
has_gpkg <- file.exists(FILE_IN_GPKG)
has_csv  <- file.exists(FILE_IN_CSV)

if (!has_gpkg && !has_csv) {
  log_error("Neither canonical input file exists. Provide at least one of:")
  log_error(glue(" - {FILE_IN_GPKG}"))
  log_error(glue(" - {FILE_IN_CSV}"))
  stop("Missing canonical inputs. See log for details.")
}

if (has_gpkg) log_info(glue("Found GPKG: {FILE_IN_GPKG}"))
if (has_csv)  log_info(glue("Found CSV:  {FILE_IN_CSV}"))

# ------------------------------------------------
# 5. Helpers (script-local only)
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

write_qc <- function(qc_tbl, file_latest, file_run = NULL) {
  safe_write_csv(qc_tbl, file_latest)
  if (!is.null(file_run)) safe_write_csv(qc_tbl, file_run)
  invisible(TRUE)
}

# align QC timestamp with log timestamp
qc_timestamp <- gsub(glue("{SCRIPT_ID}_|\\.log$"), "", basename(log_meta$log_file))
FILE_QC_RUN  <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_{qc_timestamp}.csv"))

normalize_names <- function(df) {
  # Works for sf and data.frame; preserves geometry if present
  nm <- names(df)
  names(df) <- janitor::make_clean_names(nm)
  df
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

summarize_core <- function(df, label) {
  # expects clean_names already applied
  check_required_cols(df, PARAMS$required_cols, context = label)
  
  # coercions (do not transform meaning; just make summaries robust)
  year_vals <- suppressWarnings(as.integer(df$year))
  state_vals <- df$hmm_state
  
  tibble::tibble(
    source         = label,
    n_rows         = nrow(df),
    n_cols         = ncol(df),
    year_min       = suppressWarnings(min(year_vals, na.rm = TRUE)),
    year_max       = suppressWarnings(max(year_vals, na.rm = TRUE)),
    n_year_unique  = dplyr::n_distinct(year_vals),
    n_lineages     = dplyr::n_distinct(df$lineage_id),
    n_states       = dplyr::n_distinct(state_vals),
    n_cbpseg       = dplyr::n_distinct(df$cbpseg),
    n_quadid       = dplyr::n_distinct(df$quadid),
    n_na_year      = sum(is.na(year_vals)),
    n_na_state     = sum(is.na(state_vals)),
    n_na_lineage   = sum(is.na(df$lineage_id)),
    n_na_cbpseg    = sum(is.na(df$cbpseg)),
    n_na_quadid    = sum(is.na(df$quadid))
  )
}

# ------------------------------------------------
# 6. Main process
# ------------------------------------------------
log_section("PROCESS")

qc <- tibble::tibble()

# ---- Read + validate CSV (if present) ----------------------------------------
csv_summary <- NULL
if (has_csv) {
  dat_csv <- with_timing("Read CSV", quote({
    readr::read_csv(FILE_IN_CSV, show_col_types = FALSE) |>
      normalize_names()
  }))
  
  log_info(glue("CSV rows loaded: {nrow(dat_csv)} | cols: {ncol(dat_csv)}"))
  csv_summary <- summarize_core(dat_csv, "csv")
  
  qc <- dplyr::bind_rows(
    qc,
    qc_row("csv_exists", TRUE),
    qc_row("csv_rows", nrow(dat_csv)),
    qc_row("csv_cols", ncol(dat_csv)),
    qc_row("csv_year_min", csv_summary$year_min),
    qc_row("csv_year_max", csv_summary$year_max),
    qc_row("csv_n_lineages", csv_summary$n_lineages),
    qc_row("csv_n_states", csv_summary$n_states),
    qc_row("csv_n_cbpseg", csv_summary$n_cbpseg),
    qc_row("csv_n_quadid", csv_summary$n_quadid)
  )
} else {
  qc <- dplyr::bind_rows(qc, qc_row("csv_exists", FALSE))
}

# ---- Read + validate GPKG (if present) ---------------------------------------
gpkg_summary <- NULL
if (has_gpkg) {
  gpkg_layers <- with_timing("List GPKG layers", quote({
    sf::st_layers(FILE_IN_GPKG)$name
  }))
  
  if (length(gpkg_layers) < 1) {
    log_error(glue("GPKG has no readable layers: {FILE_IN_GPKG}"))
    stop("GPKG has no readable layers: ", FILE_IN_GPKG)
  }
  
  # Default behavior: read the first layer (traditional + predictable).
  # If you later want to freeze a specific layer name, we can do that in README.
  layer_use <- gpkg_layers[1]
  log_info(glue("GPKG layers found: {paste(gpkg_layers, collapse = ', ')}"))
  log_info(glue("Using GPKG layer: {layer_use}"))
  
  dat_gpkg <- with_timing("Read GPKG layer", quote({
    sf::st_read(FILE_IN_GPKG, layer = layer_use, quiet = TRUE) |>
      normalize_names()
  }))
  
  log_info(glue("GPKG rows loaded: {nrow(dat_gpkg)} | cols: {ncol(dat_gpkg)}"))
  gpkg_summary <- summarize_core(dat_gpkg, "gpkg")
  
  qc <- dplyr::bind_rows(
    qc,
    qc_row("gpkg_exists", TRUE),
    qc_row("gpkg_layer_used", layer_use),
    qc_row("gpkg_rows", nrow(dat_gpkg)),
    qc_row("gpkg_cols", ncol(dat_gpkg)),
    qc_row("gpkg_year_min", gpkg_summary$year_min),
    qc_row("gpkg_year_max", gpkg_summary$year_max),
    qc_row("gpkg_n_lineages", gpkg_summary$n_lineages),
    qc_row("gpkg_n_states", gpkg_summary$n_states),
    qc_row("gpkg_n_cbpseg", gpkg_summary$n_cbpseg),
    qc_row("gpkg_n_quadid", gpkg_summary$n_quadid)
  )
} else {
  qc <- dplyr::bind_rows(qc, qc_row("gpkg_exists", FALSE))
}

# ---- Cross-source agreement checks (only if both exist) ----------------------
if (!is.null(csv_summary) && !is.null(gpkg_summary)) {
  log_section("CROSS-SOURCE CHECKS")
  
  # Simple, audit-friendly checks (no strict equality assumptions unless needed)
  qc <- dplyr::bind_rows(
    qc,
    qc_row("both_sources_present", TRUE),
    qc_row("csv_vs_gpkg_year_min_same", csv_summary$year_min == gpkg_summary$year_min),
    qc_row("csv_vs_gpkg_year_max_same", csv_summary$year_max == gpkg_summary$year_max)
  )
  
  if (csv_summary$year_min != gpkg_summary$year_min || csv_summary$year_max != gpkg_summary$year_max) {
    log_info(glue(
      "Year span differs (CSV {csv_summary$year_min}-{csv_summary$year_max}) vs ",
      "(GPKG {gpkg_summary$year_min}-{gpkg_summary$year_max})."
    ))
  }
} else {
  qc <- dplyr::bind_rows(qc, qc_row("both_sources_present", FALSE))
}

# ---- Session info / package versions ----------------------------------------
log_section("SESSION INFO")
si <- utils::capture.output(sessionInfo())
# keep log readable; write as a block
for (line in si) log_info(line)

qc <- dplyr::bind_rows(
  qc,
  qc_row("r_version", R.version.string),
  qc_row("platform", R.version$platform)
)

# ------------------------------------------------
# 7. Write outputs (QC only; no processed data)
# ------------------------------------------------
log_section("OUTPUTS")

with_timing("Write QC files", quote({
  write_qc(qc, file_latest = FILE_QC_LATEST, file_run = FILE_QC_RUN)
}))

log_info(glue("QC latest: {FILE_QC_LATEST}"))
log_info(glue("QC run:    {FILE_QC_RUN}"))

# ------------------------------------------------
# 8. Done
# ------------------------------------------------
log_section("DONE")
log_info("Input validation completed successfully.")