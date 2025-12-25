# ==============================================================================
# Script Title: n1_01_identity_persistence
# Purpose: Compute "Identity (Persistence)" resilience metrics per lineage from
#          the canonical Stage 0 long table (lineage × year × state_bin7).
# Author: Jason Kenosky
# Last Updated: 2025-12-16
#
# Inputs:
#   outputs/data/n0_02d_build_lineage_timeseries_table_long_latest.csv
#
# Outputs:
#   outputs/metrics/n1_01_identity_persistence_metrics_latest.csv
#   outputs/metrics/n1_01_identity_persistence_metrics_<timestamp>.csv
#   outputs/logs/n1_01_identity_persistence_<timestamp>.log
# ==============================================================================

rm(list = ls())
gc()
options(scipen = 999, dplyr.summarise.inform = FALSE)

suppressPackageStartupMessages({
  library(dplyr)
  library(readr)
  library(glue)
  library(fs)
  library(here)
  library(janitor)
  library(tibble)
})

SCRIPT_ID <- "n1_01_identity_persistence"

DIR_ROOT    <- here::here()
DIR_OUTPUTS <- file.path(DIR_ROOT, "outputs")
DIR_LOGS    <- file.path(DIR_OUTPUTS, "logs")
DIR_DATA    <- file.path(DIR_OUTPUTS, "data")
DIR_METRICS <- file.path(DIR_OUTPUTS, "metrics")

fs::dir_create(DIR_LOGS,    recurse = TRUE)
fs::dir_create(DIR_DATA,    recurse = TRUE)
fs::dir_create(DIR_METRICS, recurse = TRUE)

FILE_TS <- file.path(DIR_DATA, "n0_02d_build_lineage_timeseries_table_long_latest.csv")
if (!file.exists(FILE_TS)) stop("Missing input: ", FILE_TS)

timestamp_tag <- format(Sys.time(), "%Y%m%d-%H%M%S")
FILE_OUT_LATEST <- file.path(DIR_METRICS, glue("{SCRIPT_ID}_metrics_latest.csv"))
FILE_OUT_RUN    <- file.path(DIR_METRICS, glue("{SCRIPT_ID}_metrics_{timestamp_tag}.csv"))

# ------------------------------------------------
# Logging 
# ------------------------------------------------
FILE_LOG_HELPERS <- here::here("R", "logger_helpers.R")
if (!file.exists(FILE_LOG_HELPERS)) stop("Missing logger helpers: ", FILE_LOG_HELPERS)
source(FILE_LOG_HELPERS)

log_meta <- start_log(SCRIPT_ID, DIR_LOGS)
on.exit(stop_log(log_meta$log_con), add = TRUE)

log_section(glue("{SCRIPT_ID} - START"))
log_info(glue("Project root: {DIR_ROOT}"))
log_info(glue("Input TS file: {FILE_TS}"))
log_info(glue("Output latest: {FILE_OUT_LATEST}"))
log_info(glue("Output run:    {FILE_OUT_RUN}"))

# ------------------------------------------------
# Helpers
# ------------------------------------------------
safe_write_csv <- function(df, path) {
  fs::dir_create(dirname(path), recurse = TRUE)
  readr::write_csv(df, path)
  invisible(path)
}

# mode with deterministic tie-break
mode1 <- function(x) {
  x <- x[!is.na(x)]
  if (length(x) == 0) return(NA)
  tab <- sort(table(x), decreasing = TRUE)
  names(tab)[1]
}

longest_run <- function(x) {
  if (length(x) == 0) return(0L)
  r <- rle(x)
  max(r$lengths)
}

longest_consecutive_year_run <- function(years_int) {
  years_int <- sort(unique(years_int[!is.na(years_int)]))
  if (length(years_int) == 0) return(0L)
  if (length(years_int) == 1) return(1L)
  diffs <- diff(years_int)
  runs <- rle(diffs == 1)
  true_runs <- runs$lengths[runs$values]
  if (length(true_runs) == 0) return(1L)
  max(true_runs) + 1L
}

# ------------------------------------------------
# Read + standardize
# ------------------------------------------------
log_section("READ + STANDARDIZE")

ts_raw <- with_timing("Read Stage 0 long table", quote({
  readr::read_csv(FILE_TS, show_col_types = FALSE) |>
    janitor::clean_names()
}))

log_info(glue("Rows: {nrow(ts_raw)} | Cols: {ncol(ts_raw)}"))
log_info(glue("Columns: {paste(names(ts_raw), collapse = ', ')}"))

required <- c("lineage_id", "year", "state_bin7")
missing_req <- setdiff(required, names(ts_raw))
if (length(missing_req) > 0) stop("Missing required columns: ", paste(missing_req, collapse = ", "))

ts_raw <- ts_raw %>%
  mutate(
    lineage_id = suppressWarnings(as.integer(lineage_id)),
    year       = suppressWarnings(as.integer(year)),
    state_obs  = suppressWarnings(as.numeric(state_bin7))
  ) %>%
  filter(!is.na(lineage_id), !is.na(year), !is.na(state_obs))

# ------------------------------------------------
# Collapse duplicate lineage_id × year rows
# ------------------------------------------------
log_section("DEDUP lineage_id × year")

dup_tbl <- ts_raw %>%
  count(lineage_id, year, name = "n") %>%
  filter(n > 1)

n_dup_keys <- nrow(dup_tbl)
n_dup_rows <- sum(dup_tbl$n) - n_dup_keys

log_info(glue("Duplicate lineage_id×year keys: {n_dup_keys}"))
log_info(glue("Extra duplicate rows to be collapsed: {n_dup_rows}"))

ts <- ts_raw %>%
  arrange(lineage_id, year) %>%
  group_by(lineage_id, year) %>%
  summarise(
    state_obs    = as.numeric(mode1(state_obs)),
    cbpseg       = dplyr::first(na.omit(cbpseg)),
    quadid       = suppressWarnings(as.integer(dplyr::first(na.omit(quadid)))),
    state        = dplyr::first(na.omit(state)),
    zone         = suppressWarnings(as.integer(dplyr::first(na.omit(zone)))),
    density_mean = suppressWarnings(as.numeric(dplyr::first(na.omit(density_mean)))),
    density_bin7 = suppressWarnings(as.numeric(dplyr::first(na.omit(density_bin7)))),
    .groups = "drop"
  )

log_info(glue("After dedup: rows = {nrow(ts)}"))

# ------------------------------------------------
# Compute N1 identity/persistence metrics
# ------------------------------------------------
log_section("COMPUTE N1 METRICS")

metrics <- with_timing("Compute per-lineage metrics", quote({
  ts %>%
    arrange(lineage_id, year) %>%
    group_by(lineage_id) %>%
    summarise(
      n_obs_years = n(),
      year_min    = min(year),
      year_max    = max(year),
      span_years  = (year_max - year_min + 1L),
      presence_prop = ifelse(span_years > 0, n_obs_years / span_years, NA_real_),
      
      longest_presence_run = longest_consecutive_year_run(year),
      
      n_transitions = ifelse(n_obs_years >= 2L, n_obs_years - 1L, 0L),
      
      self_transition_rate = {
        if (n_obs_years < 2L) NA_real_
        else mean(state_obs[-1] == state_obs[-length(state_obs)], na.rm = TRUE)
      },
      
      n_state_changes = {
        if (n_obs_years < 2L) 0L
        else sum(state_obs[-1] != state_obs[-length(state_obs)], na.rm = TRUE)
      },
      
      longest_same_state_run = longest_run(state_obs),
      
      .groups = "drop"
    )
}))

log_info(glue("Computed metrics for {nrow(metrics)} lineages"))
log_info(glue("Obs years median: {median(metrics$n_obs_years)} | max: {max(metrics$n_obs_years)}"))
log_info(glue("Presence_prop median: {round(median(metrics$presence_prop, na.rm = TRUE), 3)}"))
log_info(glue("Self-transition median: {round(median(metrics$self_transition_rate, na.rm = TRUE), 3)}"))

# ------------------------------------------------
# Write outputs
# ------------------------------------------------
log_section("WRITE OUTPUTS")

with_timing("Write metrics CSVs", quote({
  safe_write_csv(metrics, FILE_OUT_LATEST)
  safe_write_csv(metrics, FILE_OUT_RUN)
}))

log_info(glue("Wrote: {FILE_OUT_LATEST}"))
log_info(glue("Wrote: {FILE_OUT_RUN}"))

log_section("DONE")
log_info("N1 identity/persistence completed successfully.")

