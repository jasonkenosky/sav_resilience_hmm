# ==============================================================================
# Script Title: n4_00_prep_hmm_inputs
# Purpose: Prepare HMM-ready sequences from Stage0 lineage × year long table.
# Author: Jason Kenosky
# Last Updated: 2025-12-16
#
# Inputs:
#   outputs/data/n0_02d_build_lineage_timeseries_table_long_latest.csv
#
# Outputs:
#   outputs/hmm/n4_00_hmm_input_long_latest.csv
#   outputs/hmm/n4_00_hmm_input_long_<timestamp>.csv
#   outputs/hmm/n4_00_hmm_input_sequences_latest.csv
#   outputs/hmm/n4_00_hmm_input_sequences_<timestamp>.csv
#   outputs/qc/n4_00_prep_hmm_inputs_qc_latest.csv
#   outputs/qc/n4_00_prep_hmm_inputs_qc_<timestamp>.csv
#   outputs/logs/n4_00_prep_hmm_inputs_<timestamp>.log
# ==============================================================================

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
  library(fs)
  library(here)
  library(janitor)
  library(tibble)
  library(stringr)
})

# ------------------------------------------------
# 2. Script configuration (directories, parameters)
# ------------------------------------------------
SCRIPT_ID <- "n4_00_prep_hmm_inputs"

DIR_ROOT    <- here::here()
DIR_OUTPUTS <- file.path(DIR_ROOT, "outputs")
DIR_LOGS    <- file.path(DIR_OUTPUTS, "logs")
DIR_QC      <- file.path(DIR_OUTPUTS, "qc")
DIR_DATA    <- file.path(DIR_OUTPUTS, "data")
DIR_HMM     <- file.path(DIR_OUTPUTS, "hmm")

fs::dir_create(DIR_LOGS, recurse = TRUE)
fs::dir_create(DIR_QC, recurse = TRUE)
fs::dir_create(DIR_HMM, recurse = TRUE)

FILE_IN <- file.path(DIR_DATA, "n0_02d_build_lineage_timeseries_table_long_latest.csv")

timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

FILE_OUT_LONG_LATEST <- file.path(DIR_HMM, glue("{SCRIPT_ID}_hmm_input_long_latest.csv"))
FILE_OUT_LONG_RUN    <- file.path(DIR_HMM, glue("{SCRIPT_ID}_hmm_input_long_{timestamp}.csv"))

FILE_OUT_SEQ_LATEST  <- file.path(DIR_HMM, glue("{SCRIPT_ID}_hmm_input_sequences_latest.csv"))
FILE_OUT_SEQ_RUN     <- file.path(DIR_HMM, glue("{SCRIPT_ID}_hmm_input_sequences_{timestamp}.csv"))

FILE_QC_LATEST       <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_latest.csv"))
FILE_QC_RUN          <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_{timestamp}.csv"))

PARAMS <- list(
  # Stage0 required
  required_cols = c("lineage_id", "year", "state_bin7"),
  
  # Freeze your observation alphabet (bin7 as characters)
  allowed_states = as.character(c(1, 1.5, 2, 2.5, 3, 3.5, 4)),
  
  # HMM gap handling:
  # - "keep_gaps"     : keep only observed years; HMM sees irregular intervals
  # - "complete_years": fill missing years with NA (explicit missing)
  gap_mode = "complete_years",
  
  # Optional filters
  min_obs_years = 5,          # drop ultra-short sequences for HMM fitting
  drop_duplicate_years = TRUE # enforce unique (lineage_id, year)
)

# ------------------------------------------------
# 3. Logging 
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
  inputs = list("Stage0 long table" = FILE_IN),
  outputs = list(
    "HMM input long (latest)" = FILE_OUT_LONG_LATEST,
    "HMM input long (run)"    = FILE_OUT_LONG_RUN,
    "HMM sequences (latest)"  = FILE_OUT_SEQ_LATEST,
    "HMM sequences (run)"     = FILE_OUT_SEQ_RUN,
    "QC (latest)"             = FILE_QC_LATEST,
    "QC (run)"                = FILE_QC_RUN
  )
)
log_params(PARAMS)

# ------------------------------------------------
# 4. Inputs (validate early)
# ------------------------------------------------
log_section("READ + VALIDATE INPUT")

dat <- with_timing("Read Stage0 long table", quote({
  readr::read_csv(FILE_IN, show_col_types = FALSE) |>
    janitor::clean_names()
}))

check_required_cols(dat, PARAMS$required_cols, context = "Stage0 long")

# choose which optional columns exist
cbpseg <- if ("cbpseg" %in% names(dat)) as.character(dat$cbpseg) else NA_character_
quadid <- if ("quadid" %in% names(dat)) as.character(dat$quadid) else NA_character_
state  <- if ("state"  %in% names(dat)) as.character(dat$state)  else NA_character_
zone   <- if ("zone"   %in% names(dat)) as.character(dat$zone)   else NA_character_

dat <- dat %>%
  transmute(
    lineage_id = suppressWarnings(as.integer(lineage_id)),
    year       = suppressWarnings(as.integer(year)),
    state_bin7 = as.character(state_bin7),
    cbpseg = cbpseg,
    quadid = quadid,
    state  = state,
    zone   = zone
  )

for (nm in c("cbpseg","quadid","state","zone")) {
  if (!nm %in% names(dat)) dat[[nm]] <- NA_character_
}

log_info(glue("Rows: {nrow(dat)} | Lineages: {n_distinct(dat$lineage_id)} | Years: {n_distinct(dat$year)}"))
log_info(glue("Year span: {min(dat$year, na.rm=TRUE)}-{max(dat$year, na.rm=TRUE)}"))

# Validate alphabet (allow NA if we later complete missing years)
bad_states <- dat %>%
  filter(!is.na(state_bin7) & !(state_bin7 %in% PARAMS$allowed_states)) %>%
  distinct(state_bin7) %>%
  pull(state_bin7)

if (length(bad_states) > 0) {
  log_error(glue("Found states not in allowed_states: {paste(bad_states, collapse = ', ')}"))
  stop("Invalid state_bin7 values detected.")
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
  tibble(
    timestamp = Sys.time(),
    script_id = SCRIPT_ID,
    metric    = metric,
    value     = as.character(value),
    notes     = notes
  )
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


# ------------------------------------------------
# 6. Main process
# ------------------------------------------------
# ---- enforce unique (lineage_id, year) -----
log_section("DEDUPLICATE (lineage_id, year)")

qc <- tibble()

dup_n <- dat %>%
  count(lineage_id, year, name = "n") %>%
  filter(n > 1) %>%
  nrow()

qc <- bind_rows(qc, qc_row("n_duplicate_lineage_year", dup_n))

if (PARAMS$drop_duplicate_years && dup_n > 0) {
  log_warn(glue("Found {dup_n} duplicated (lineage_id, year). Keeping first after arranging."))
  dat <- dat %>%
    arrange(lineage_id, year) %>%
    distinct(lineage_id, year, .keep_all = TRUE)
}

# ---- gap handling ----
log_section("GAP HANDLING")

year_min <- min(dat$year, na.rm = TRUE)
year_max <- max(dat$year, na.rm = TRUE)

if (PARAMS$gap_mode == "complete_years") {
  dat_hmm <- dat %>%
    group_by(lineage_id) %>%
    tidyr::complete(year = year_min:year_max) %>%
    ungroup() %>%
    arrange(lineage_id, year)
} else {
  dat_hmm <- dat %>%
    arrange(lineage_id, year)
}

qc <- bind_rows(
  qc,
  qc_row("gap_mode", PARAMS$gap_mode),
  qc_row("year_min_global", year_min),
  qc_row("year_max_global", year_max),
  qc_row("rows_after_gap_handling", nrow(dat_hmm))
)

# ---- filter short sequences ----
log_section("FILTER SHORT SEQUENCES")

obs_counts <- dat_hmm %>%
  group_by(lineage_id) %>%
  summarise(n_obs = sum(!is.na(state_bin7)), .groups = "drop")

keep_ids <- obs_counts %>%
  filter(n_obs >= PARAMS$min_obs_years) %>%
  pull(lineage_id)

qc <- bind_rows(
  qc,
  qc_row("min_obs_years", PARAMS$min_obs_years),
  qc_row("n_lineages_before_filter", n_distinct(dat_hmm$lineage_id)),
  qc_row("n_lineages_after_filter", length(keep_ids))
)

dat_hmm <- dat_hmm %>%
  filter(lineage_id %in% keep_ids)

# ---- build sequences table ----
log_section("BUILD SEQUENCE STRINGS")

seq_tbl <- dat_hmm %>%
  mutate(state_bin7 = if_else(is.na(state_bin7), NA_character_, state_bin7)) %>%
  group_by(lineage_id) %>%
  summarise(
    year_min = min(year, na.rm = TRUE),
    year_max = max(year, na.rm = TRUE),
    n_steps  = n(),
    n_obs    = sum(!is.na(state_bin7)),
    seq      = paste0(if_else(is.na(state_bin7), "NA", state_bin7), collapse = " "),
    .groups  = "drop"
  )

qc <- bind_rows(
  qc,
  qc_row("hmm_long_rows", nrow(dat_hmm)),
  qc_row("hmm_seq_rows", nrow(seq_tbl)),
  qc_row("hmm_seq_n_obs_median", median(seq_tbl$n_obs)),
  qc_row("hmm_seq_n_obs_p90", quantile(seq_tbl$n_obs, 0.9))
)

# ------------------------------------------------
# 7. Write outputs
# ------------------------------------------------
log_section("WRITE OUTPUTS")

with_timing("Write HMM inputs + QC", quote({
  safe_write_csv(dat_hmm, FILE_OUT_LONG_LATEST)
  safe_write_csv(dat_hmm, FILE_OUT_LONG_RUN)
  safe_write_csv(seq_tbl, FILE_OUT_SEQ_LATEST)
  safe_write_csv(seq_tbl, FILE_OUT_SEQ_RUN)
  safe_write_csv(qc, FILE_QC_LATEST)
  safe_write_csv(qc, FILE_QC_RUN)
}))

# ------------------------------------------------
# 8. QC summary 
# ------------------------------------------------

# ------------------------------------------------
# 9. Done
# ------------------------------------------------

log_section("DONE")
log_info("N4 prep completed successfully.")
