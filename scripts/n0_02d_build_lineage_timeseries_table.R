# ==============================================================================
# Script Title: n0_02d_build_lineage_timeseries_table
# Purpose: Build Stage 0 lineage × year time series table for downstream
#          resilience metrics (pre-HMM).
# Author: Jason Kenosky
# Last Updated: 2025-12-15
#
# Description:
#   - Reads lineage inputs (CSV but falls back to GPKG if you are on a Mac and 
#     having Numbers issues)
#   - Validates required columns
#   - Derives density_bin7 by rounding density_mean to nearest:
#       1, 1.5, 2, 2.5, 3, 3.5, 4
#   - Produces one long table: lineage_id × year × state (+ group vars)
#   - Writes QC summaries + outputs for downstream scripts
#
# Inputs:
#   data/data_raw/sav_superpatch_lineages_stable.csv (preferred)
#   data/data_raw/sav_superpatch_lineages_stable.gpkg (fallback)
#
# Outputs:
#   outputs/data/n0_02d_lineage_timeseries_long_latest.csv
#   outputs/data/n0_02d_lineage_timeseries_long_<timestamp>.csv
#   outputs/qc/n0_02d_build_lineage_timeseries_table_qc_latest.csv
#   outputs/qc/n0_02d_build_lineage_timeseries_table_qc_<timestamp>.csv
#   outputs/logs/n0_02d_build_lineage_timeseries_table_<timestamp>.log
# ==============================================================================

rm(list = ls())
gc()

# sink reset
while (sink.number(type = "output") > 0) sink(NULL)


options(
  scipen = 999,
  dplyr.summarise.inform = FALSE
)

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glue)
  library(fs)
  library(here)
  library(janitor)
  library(tibble)
  library(sf)
})

# ------------------------------------------------
# 1. Script configuration
# ------------------------------------------------
SCRIPT_ID <- "n0_02d_build_lineage_timeseries_table"

DIR_ROOT <- here::here()

DIR_DATA_RAW <- file.path(DIR_ROOT, "data", "data_raw")
DIR_OUTPUTS  <- file.path(DIR_ROOT, "outputs")
DIR_LOGS     <- file.path(DIR_OUTPUTS, "logs")
DIR_QC       <- file.path(DIR_OUTPUTS, "qc")
DIR_DATA_OUT <- file.path(DIR_OUTPUTS, "data")

fs::dir_create(DIR_DATA_RAW, recurse = TRUE)
fs::dir_create(DIR_OUTPUTS,  recurse = TRUE)
fs::dir_create(DIR_LOGS,     recurse = TRUE)
fs::dir_create(DIR_QC,       recurse = TRUE)
fs::dir_create(DIR_DATA_OUT, recurse = TRUE)

FILE_IN_CSV  <- file.path(DIR_DATA_RAW, "sav_superpatch_lineages_stable.csv")
FILE_IN_GPKG <- file.path(DIR_DATA_RAW, "sav_superpatch_lineages_stable.gpkg")

FILE_OUT_LATEST <- file.path(DIR_DATA_OUT, glue("{SCRIPT_ID}_long_latest.csv"))
FILE_QC_LATEST  <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_latest.csv"))

PARAMS <- list(
  required_cols = c("lineage_id", "year", "density_mean", "cbpseg", "quadid"),
  keep_cols = c("lineage_id","year","density_mean","cbpseg","quadid","state","zone"),
  density_bin7_levels = c(1, 1.5, 2, 2.5, 3, 3.5, 4),
  years_expected = 1984:2023
)

# ------------------------------------------------
# 2. Logging 
# ------------------------------------------------
FILE_LOG_HELPERS <- here::here("R", "logger_helpers.R")
if (!file.exists(FILE_LOG_HELPERS)) stop("Missing logger helpers: ", FILE_LOG_HELPERS)
source(FILE_LOG_HELPERS)

log_meta <- start_log(SCRIPT_ID, DIR_LOGS)
on.exit(stop_log(log_meta$log_con), add = TRUE)

log_section(glue("{SCRIPT_ID} - START"))
log_info(glue("Project root: {DIR_ROOT}"))
log_info(glue("Input CSV:    {FILE_IN_CSV}"))
log_info(glue("Input GPKG:   {FILE_IN_GPKG}"))
log_info(glue("Output latest:{FILE_OUT_LATEST}"))

# align QC timestamp with log timestamp
qc_timestamp <- gsub(glue("{SCRIPT_ID}_|\\.log$"), "", basename(log_meta$log_file))
FILE_OUT_RUN <- file.path(DIR_DATA_OUT, glue("{SCRIPT_ID}_long_{qc_timestamp}.csv"))
FILE_QC_RUN  <- file.path(DIR_QC,      glue("{SCRIPT_ID}_qc_{qc_timestamp}.csv"))

# ------------------------------------------------
# 3. Helpers
# ------------------------------------------------
normalize_names <- function(df) {
  names(df) <- janitor::make_clean_names(names(df))
  df
}

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

# ------------------------------------------------
# 4. Read input (CSV preferred)
# ------------------------------------------------
log_section("INPUTS")

has_csv  <- file.exists(FILE_IN_CSV)
has_gpkg <- file.exists(FILE_IN_GPKG)

if (!has_csv && !has_gpkg) {
  stop("Neither input exists. Provide at least one of:\n - ", FILE_IN_CSV, "\n - ", FILE_IN_GPKG)
}

source_used <- NA_character_
dat <- NULL

if (has_csv) {
  log_info("Reading CSV (preferred).")
  dat <- with_timing("Read CSV", quote({
    readr::read_csv(FILE_IN_CSV, show_col_types = FALSE) |> normalize_names()
  }))
  source_used <- "csv"
} else {
  log_info("CSV not found; reading GPKG.")
  layers <- sf::st_layers(FILE_IN_GPKG)$name
  if (length(layers) < 1) stop("No layers in GPKG: ", FILE_IN_GPKG)
  layer_use <- layers[1]
  log_info(glue("Using GPKG layer: {layer_use}"))
  dat <- with_timing("Read GPKG", quote({
    sf::st_read(FILE_IN_GPKG, layer = layer_use, quiet = TRUE) |>
      sf::st_drop_geometry() |>
      as.data.frame() |>
      normalize_names()
  }))
  source_used <- "gpkg"
}

log_info(glue("Loaded {source_used}: rows={nrow(dat)} cols={ncol(dat)}"))
log_info(glue("Column names (first 12): {paste(head(names(dat), 12), collapse = ', ')}"))

check_required_cols(dat, PARAMS$required_cols, context = source_used)

# ------------------------------------------------
# 5. Build downstream long table
# ------------------------------------------------
log_section("BUILD LONG TABLE")

# keep columns 
keep <- intersect(PARAMS$keep_cols, names(dat))

ts_long <- dat %>%
  dplyr::select(dplyr::all_of(keep)) %>%
  dplyr::mutate(
    year = suppressWarnings(as.integer(year)),
    density_mean = suppressWarnings(as.numeric(density_mean)),
    density_bin7 = bin_density7(density_mean, PARAMS$density_bin7_levels)
  ) %>%
  dplyr::mutate(
    # canonical "state" for Stage 0
    state_bin7 = as.character(density_bin7)
  ) %>%
  dplyr::select(
    lineage_id,
    year,
    state_bin7,
    density_mean,
    density_bin7,
    dplyr::any_of(c("cbpseg", "quadid", "state", "zone"))
  ) %>%
  dplyr::arrange(lineage_id, year)

# hard stop if missing values
stopifnot(!any(is.na(ts_long$lineage_id)))
stopifnot(!any(is.na(ts_long$year)))
stopifnot(!any(is.na(ts_long$state_bin7)))

# ------------------------------------------------
# 6. Enforce uniqueness: one row per lineage_id × year
# ------------------------------------------------

mode1 <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA)
  tab <- sort(table(x), decreasing = TRUE)
  names(tab)[1]
}

bin_density7 <- function(x, levels = c(1, 1.5, 2, 2.5, 3, 3.5, 4)) {
  x <- suppressWarnings(as.numeric(x))
  out <- rep(NA_real_, length(x))
  ok <- !is.na(x)
  if (any(ok)) {
    lv <- as.numeric(levels)
    idx <- vapply(x[ok], function(v) which.min(abs(lv - v)), integer(1))
    out[ok] <- lv[idx]
  }
  out
}

# quick log counts 
dup_keys <- ts_long %>% count(lineage_id, year, name="n") %>% filter(n > 1)
log_info(glue("Duplicate lineage_id×year keys: {nrow(dup_keys)}"))

ts_long_unique <- ts_long %>%
  janitor::clean_names() %>%
  mutate(
    lineage_id   = suppressWarnings(as.integer(lineage_id)),
    year         = suppressWarnings(as.integer(year)),
    state_bin7   = suppressWarnings(as.numeric(state_bin7)),
    density_mean = suppressWarnings(as.numeric(density_mean)),
    density_bin7 = suppressWarnings(as.numeric(density_bin7))
  ) %>%
  arrange(lineage_id, year) %>%
  group_by(lineage_id, year) %>%
  summarise(
    # freeze Stage 0 state
    state_bin7 = as.numeric(mode1(state_bin7)),
    
    # aggregate density_mean then re-bin so it is consistent
    density_mean = mean(density_mean, na.rm = TRUE),
    density_bin7 = bin_density7(density_mean),
    
    # conservative carry-through for grouping vars
    cbpseg = dplyr::first(na.omit(cbpseg)),
    quadid = suppressWarnings(as.integer(dplyr::first(na.omit(quadid)))),
    state  = dplyr::first(na.omit(state)),
    zone   = suppressWarnings(as.integer(dplyr::first(na.omit(zone)))),
    
    .groups = "drop"
  )

ts_long <- ts_long_unique
rm(ts_long_unique)

# ------------------------------------------------
# 7. QC summaries
# ------------------------------------------------
log_section("QC")

qc <- tibble::tibble()

years_obs <- sort(unique(ts_long$year))
missing_years <- setdiff(PARAMS$years_expected, years_obs)

qc <- dplyr::bind_rows(
  qc,
  qc_row("source_used", source_used),
  qc_row("n_rows", nrow(ts_long)),
  qc_row("n_lineages", dplyr::n_distinct(ts_long$lineage_id)),
  qc_row("year_min", min(ts_long$year, na.rm = TRUE)),
  qc_row("year_max", max(ts_long$year, na.rm = TRUE)),
  qc_row("n_year_unique", dplyr::n_distinct(ts_long$year)),
  qc_row("missing_years_count", length(missing_years)),
  qc_row("missing_years", if (length(missing_years) == 0) "none" else paste(missing_years, collapse = ",")),
  qc_row("n_cbpseg", if ("cbpseg" %in% names(ts_long)) dplyr::n_distinct(ts_long$cbpseg) else NA),
  qc_row("n_quadid", if ("quadid" %in% names(ts_long)) dplyr::n_distinct(ts_long$quadid) else NA),
  qc_row("n_state_bin7", dplyr::n_distinct(ts_long$state_bin7)),
  qc_row("density_outside_1_4", sum(ts_long$density_mean < 1 | ts_long$density_mean > 4, na.rm = TRUE))
)

bin_counts <- ts_long %>%
  count(state_bin7, name = "n") %>%
  arrange(as.numeric(state_bin7))

log_section("STATE_BIN7 DISTRIBUTION")
apply(bin_counts, 1, function(r) log_info(glue("state={r[['state_bin7']]} | n={r[['n']]}")))

# ------------------------------------------------
# 8. Write outputs
# ------------------------------------------------
log_section("OUTPUTS")

with_timing("Write long table", quote({
  safe_write_csv(ts_long, FILE_OUT_LATEST)
  safe_write_csv(ts_long, FILE_OUT_RUN)
}))

with_timing("Write QC files", quote({
  write_qc(qc, FILE_QC_LATEST, FILE_QC_RUN)
}))

log_info(glue("Wrote long latest: {FILE_OUT_LATEST}"))
log_info(glue("Wrote long run:    {FILE_OUT_RUN}"))
log_info(glue("QC latest:         {FILE_QC_LATEST}"))
log_info(glue("QC run:            {FILE_QC_RUN}"))

log_section("DONE")
log_info("Stage 0 canonical lineage time series table built successfully.")
