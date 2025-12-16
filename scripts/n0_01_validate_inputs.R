# ==============================================================================
# Script Title: n0_01_validate_inputs
# Purpose: Validate canonical SAV lineage inputs (pre-HMM) and derive the Stage 0
#          7-bin density alphabet used for downstream time series construction.
# Author: Jason Kenosky
# Last Updated: 2025-12-15
#
# Description:
#   - Confirms canonical lineage inputs (GPKG and/or CSV) exist
#   - Validates required columns for Stage 0 (pre-HMM)
#   - Derives density_bin7 by rounding density_mean to nearest:
#       1, 1.5, 2, 2.5, 3, 3.5, 4
#   - Reports key counts and coverage (years, lineages, density bins, CBPSEG, QUADID)
#   - Writes QC summary tables and a log for auditability
#
# Inputs:
#   data/data_raw/sav_superpatch_lineages_stable.gpkg
#   data/data_raw/sav_superpatch_lineages_stable.csv
#
# Outputs:
#   outputs/qc/n0_01_validate_inputs_qc_latest.csv
#   outputs/qc/n0_01_validate_inputs_qc_<timestamp>.csv
#   outputs/logs/n0_01_validate_inputs_<timestamp>.log
# ==============================================================================

# ------------------------------------------------
# 0. Housekeeping (clean session friendly)
# ------------------------------------------------
rm(list = ls())  # Only clears items in Global Environment, it might not clear
gc()             # all items in the session.  Some, like dplyr, might linger.

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
fs::dir_create(DIR_DATA_RAW, recurse = TRUE)
fs::dir_create(DIR_DATA_PROCESSED, recurse = TRUE)
fs::dir_create(DIR_OUTPUTS, recurse = TRUE)
fs::dir_create(DIR_LOGS, recurse = TRUE)
fs::dir_create(DIR_QC, recurse = TRUE)

# Canonical Stage 0 inputs for THIS repo
FILE_IN_GPKG <- file.path(DIR_DATA_RAW, "sav_superpatch_lineages_stable.gpkg")
FILE_IN_CSV  <- file.path(DIR_DATA_RAW, "sav_superpatch_lineages_stable.csv")

# QC files
FILE_QC_LATEST <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_latest.csv"))

# Stage 0 required columns (pre-HMM)
PARAMS <- list(
  required_cols = c("lineage_id", "year", "density_mean", "cbpseg", "quadid"),
  density_bin7_levels = c(1, 1.5, 2, 2.5, 3, 3.5, 4),
  gpkg_path = FILE_IN_GPKG,
  csv_path  = FILE_IN_CSV
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
log_info(glue("Required columns (Stage 0): {paste(PARAMS$required_cols, collapse = ', ')}"))
log_info(glue("Density bin7 levels:        {paste(PARAMS$density_bin7_levels, collapse = ', ')}"))
log_info(glue("Expected GPKG:              {PARAMS$gpkg_path}"))
log_info(glue("Expected CSV:               {PARAMS$csv_path}"))

# ------------------------------------------------
# 4. Inputs (validate early)
# ------------------------------------------------
log_section("INPUTS")

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

if (has_csv) {
  log_info(glue("CSV size bytes: {file.size(FILE_IN_CSV)}"))
  log_info(glue("CSV header raw: {readLines(FILE_IN_CSV, n = 1)}"))
}

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
  nm <- names(df)
  names(df) <- janitor::make_clean_names(nm)
  df
}

check_required_cols <- function(df, required_cols, context = "data") {
  
  # helpful warning BEFORE stopping
  if (ncol(df) == 1) {
    log_warn(glue("Only 1 column detected in {context}. Likely delimiter/export issue."))
  }
  
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    msg <- glue("Missing required columns in {context}: {paste(missing, collapse = ', ')}")
    log_error(msg)
    stop(msg)
  }
  
  invisible(TRUE)
}

# Round density_mean to nearest of 1,1.5,2,2.5,3,3.5,4
bin_density7 <- function(x, levels = c(1, 1.5, 2, 2.5, 3, 3.5, 4)) {
  x <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(x))
  ok <- !is.na(x)
  if (any(ok)) {
    lv <- as.numeric(levels)
    idx <- vapply(
      x[ok],
      function(v) which.min(abs(lv - v)),
      integer(1)
    )
    out[ok] <- lv[idx]
  }
  out
}

summarize_stage0 <- function(df, label) {
  check_required_cols(df, PARAMS$required_cols, context = label)
  
  year_vals <- suppressWarnings(as.integer(df$year))
  dens_vals <- suppressWarnings(as.numeric(df$density_mean))
  
  df$density_bin7 <- bin_density7(dens_vals, PARAMS$density_bin7_levels)
  
  n_outside <- sum(!is.na(dens_vals) & (dens_vals < 1 | dens_vals > 4), na.rm = TRUE)
  n_missing_bin <- sum(is.na(df$density_bin7))
  
  bad_bin <- sum(!is.na(df$density_bin7) & !(df$density_bin7 %in% PARAMS$density_bin7_levels))
  
  list(
    summary = tibble::tibble(
      source         = label,
      n_rows         = nrow(df),
      n_cols         = ncol(df),
      year_min       = suppressWarnings(min(year_vals, na.rm = TRUE)),
      year_max       = suppressWarnings(max(year_vals, na.rm = TRUE)),
      n_year_unique  = dplyr::n_distinct(year_vals),
      n_lineages     = dplyr::n_distinct(df$lineage_id),
      n_cbpseg       = dplyr::n_distinct(df$cbpseg),
      n_quadid       = dplyr::n_distinct(df$quadid),
      n_bin7         = dplyr::n_distinct(df$density_bin7),
      n_na_year      = sum(is.na(year_vals)),
      n_na_density   = sum(is.na(dens_vals)),
      n_na_cbpseg    = sum(is.na(df$cbpseg)),
      n_na_quadid    = sum(is.na(df$quadid)),
      n_density_outside_1_4 = n_outside,
      n_density_bin7_na     = n_missing_bin,
      n_density_bin7_invalid = bad_bin
    ),
    bin_counts = df %>%
      dplyr::count(density_bin7, name = "n") %>%
      dplyr::arrange(density_bin7)
  )
}

check_required_cols <- function(df, required_cols, context = "data") {
  
  if (is.null(df)) {
    log_error(glue("{context} object is NULL"))
    stop(glue("{context} object is NULL"))
  }
  
  if (!is.data.frame(df)) {
    log_error(glue("{context} is not a data.frame (class: {class(df)})"))
    stop(glue("{context} is not a data.frame"))
  }
  
  if (ncol(df) == 1) {
    log_warn(glue("Only 1 column detected in {context}. Likely delimiter/export issue."))
  }
  
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    msg <- glue("Missing required columns in {context}: {paste(missing, collapse = ', ')}")
    log_error(msg)
    stop(msg)
  }
  
  invisible(TRUE)
}

# ------------------------------------------------
# 6. Main process
# ------------------------------------------------
log_section("PROCESS")

qc <- tibble::tibble()
csv_res <- NULL
gpkg_res <- NULL

# ---- CSV --------------------------------------------------------------------
if (has_csv) {
  
  dat_csv <- with_timing("Read CSV/TSV (auto-detect delimiter)", quote({
    
    first_line <- readLines(FILE_IN_CSV, n = 1)
    
    dat_in <- if (grepl("\t", first_line) && !grepl(",", first_line)) {
      log_info("Detected tab-delimited file; reading with readr::read_tsv().")
      readr::read_tsv(FILE_IN_CSV, show_col_types = FALSE)
    } else {
      log_info("Detected comma-delimited file; reading with readr::read_csv().")
      readr::read_csv(FILE_IN_CSV, show_col_types = FALSE)
    }
    
    log_info(glue("CSV raw names (first 10):   {paste(head(names(dat_in), 10), collapse = ', ')}"))
    
    dat_out <- dat_in |> normalize_names()
    
    log_info(glue("CSV clean names (first 10): {paste(head(names(dat_out), 10), collapse = ', ')}"))
    
    dat_out
  }))
  
  if (is.null(dat_csv)) {
    log_error("CSV read returned NULL — with_timing() did not return a value.")
    stop("CSV read failed: dat_csv is NULL")
  }
  
  log_info(glue("CSV rows loaded: {nrow(dat_csv)} | cols: {ncol(dat_csv)}"))
  
  csv_res <- summarize_stage0(dat_csv, "csv")
  
  log_info(glue("CSV year span: {csv_res$summary$year_min}-{csv_res$summary$year_max}"))
  log_info(glue("CSV lineages:  {csv_res$summary$n_lineages}"))
  log_info(glue("CSV cbpseg:    {csv_res$summary$n_cbpseg} | quadid: {csv_res$summary$n_quadid}"))
  
  log_section("CSV DENSITY_BIN7 DISTRIBUTION")
  apply(csv_res$bin_counts, 1, function(r) log_info(glue("bin={r[['density_bin7']]} | n={r[['n']]}")))
  
  qc <- dplyr::bind_rows(
    qc,
    qc_row("csv_exists", TRUE),
    qc_row("csv_rows", csv_res$summary$n_rows),
    qc_row("csv_cols", csv_res$summary$n_cols),
    qc_row("csv_year_min", csv_res$summary$year_min),
    qc_row("csv_year_max", csv_res$summary$year_max),
    qc_row("csv_n_lineages", csv_res$summary$n_lineages),
    qc_row("csv_n_cbpseg", csv_res$summary$n_cbpseg),
    qc_row("csv_n_quadid", csv_res$summary$n_quadid),
    qc_row("csv_n_bin7", csv_res$summary$n_bin7),
    qc_row("csv_n_density_outside_1_4", csv_res$summary$n_density_outside_1_4),
    qc_row("csv_n_density_bin7_na", csv_res$summary$n_density_bin7_na),
    qc_row("csv_n_density_bin7_invalid", csv_res$summary$n_density_bin7_invalid)
  )
  
} else {
  qc <- dplyr::bind_rows(qc, qc_row("csv_exists", FALSE))
}

# ---- GPKG -------------------------------------------------------------------
if (has_gpkg) {
  
  gpkg_layers <- with_timing("List GPKG layers", quote({
    sf::st_layers(FILE_IN_GPKG)$name
  }))
  
  if (length(gpkg_layers) < 1) {
    log_error(glue("GPKG has no readable layers: {FILE_IN_GPKG}"))
    stop("GPKG has no readable layers: ", FILE_IN_GPKG)
  }
  
  layer_use <- gpkg_layers[1]
  log_info(glue("GPKG layers found: {paste(gpkg_layers, collapse = ', ')}"))
  log_info(glue("Using GPKG layer: {layer_use}"))
  
  dat_gpkg <- with_timing("Read GPKG layer", quote({
    sf::st_read(FILE_IN_GPKG, layer = layer_use, quiet = TRUE) |>
      sf::st_drop_geometry() |>
      as.data.frame() |>
      normalize_names()
  }))
  
  log_info(glue("GPKG rows loaded: {nrow(dat_gpkg)} | cols: {ncol(dat_gpkg)}"))
  log_info(glue("GPKG clean names (first 10): {paste(head(names(dat_gpkg), 10), collapse = ', ')}"))
  
  gpkg_res <- summarize_stage0(dat_gpkg, "gpkg")
  
  log_info(glue("GPKG year span: {gpkg_res$summary$year_min}-{gpkg_res$summary$year_max}"))
  log_info(glue("GPKG lineages:  {gpkg_res$summary$n_lineages}"))
  log_info(glue("GPKG cbpseg:    {gpkg_res$summary$n_cbpseg} | quadid: {gpkg_res$summary$n_quadid}"))
  
  log_section("GPKG DENSITY_BIN7 DISTRIBUTION")
  apply(gpkg_res$bin_counts, 1, function(r) log_info(glue("bin={r[['density_bin7']]} | n={r[['n']]}")))
  
  qc <- dplyr::bind_rows(
    qc,
    qc_row("gpkg_exists", TRUE),
    qc_row("gpkg_layer_used", layer_use),
    qc_row("gpkg_rows", gpkg_res$summary$n_rows),
    qc_row("gpkg_cols", gpkg_res$summary$n_cols),
    qc_row("gpkg_year_min", gpkg_res$summary$year_min),
    qc_row("gpkg_year_max", gpkg_res$summary$year_max),
    qc_row("gpkg_n_lineages", gpkg_res$summary$n_lineages),
    qc_row("gpkg_n_cbpseg", gpkg_res$summary$n_cbpseg),
    qc_row("gpkg_n_quadid", gpkg_res$summary$n_quadid),
    qc_row("gpkg_n_bin7", gpkg_res$summary$n_bin7),
    qc_row("gpkg_n_density_outside_1_4", gpkg_res$summary$n_density_outside_1_4),
    qc_row("gpkg_n_density_bin7_na", gpkg_res$summary$n_density_bin7_na),
    qc_row("gpkg_n_density_bin7_invalid", gpkg_res$summary$n_density_bin7_invalid)
  )
  
} else {
  qc <- dplyr::bind_rows(qc, qc_row("gpkg_exists", FALSE))
}

# ---- Cross-source checks -----------------------------------------------------
if (!is.null(csv_res) && !is.null(gpkg_res)) {
  log_section("CROSS-SOURCE CHECKS")
  
  qc <- dplyr::bind_rows(
    qc,
    qc_row("both_sources_present", TRUE),
    qc_row("csv_vs_gpkg_year_min_same", csv_res$summary$year_min == gpkg_res$summary$year_min),
    qc_row("csv_vs_gpkg_year_max_same", csv_res$summary$year_max == gpkg_res$summary$year_max),
    qc_row("csv_vs_gpkg_n_lineages_same", csv_res$summary$n_lineages == gpkg_res$summary$n_lineages)
  )
} else {
  qc <- dplyr::bind_rows(qc, qc_row("both_sources_present", FALSE))
}

# ---- Session info ------------------------------------------------------------
log_section("SESSION INFO")
si <- utils::capture.output(sessionInfo())
for (line in si) log_info(line)

qc <- dplyr::bind_rows(
  qc,
  qc_row("r_version", R.version.string),
  qc_row("platform", R.version$platform)
)

# ------------------------------------------------
# 7. Write outputs (QC only)
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
log_info("Stage 0 input validation completed successfully.")