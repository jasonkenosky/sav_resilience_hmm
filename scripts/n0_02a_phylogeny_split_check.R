# ==============================================================================
# Script Title: n0_02_phylogeny_split_check
# Purpose: Stage 0 inspect lineage topology for SPLITS using the
#          stable lineage table (friendly view; no merge modeling).
# Author: Jason Kenosky
# Last Updated: 2025-12-15
#
# Inputs:
#   data/data_raw/sav_superpatch_lineages_stable.csv
#
# Outputs (QC only):
#   outputs/qc/n0_02_phylogeny_split_check_overall_latest.csv
#   outputs/qc/n0_02_phylogeny_split_check_split_events_latest.csv
#   outputs/qc/n0_02_phylogeny_split_check_split_summary_latest.csv
#   outputs/qc/n0_02_phylogeny_split_check_sample400_latest.csv
#   outputs/logs/n0_02_phylogeny_split_check_<timestamp>.log
# ==============================================================================

rm(list = ls()); 
gc()

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
})

# ------------------------------------------------
# 1. Script configuration
# ------------------------------------------------
SCRIPT_ID <- "n0_02_phylogeny_split_check"

DIR_ROOT <- here::here()
DIR_DATA_RAW <- file.path(DIR_ROOT, "data", "data_raw")
DIR_OUTPUTS <- file.path(DIR_ROOT, "outputs")
DIR_LOGS <- file.path(DIR_OUTPUTS, "logs")
DIR_QC <- file.path(DIR_OUTPUTS, "qc")

fs::dir_create(DIR_LOGS, recurse = TRUE)
fs::dir_create(DIR_QC, recurse = TRUE)

FILE_IN <- file.path(DIR_DATA_RAW, "sav_superpatch_lineages_stable.csv")

FILE_OVERALL      <- file.path(DIR_QC, glue("{SCRIPT_ID}_overall_latest.csv"))
FILE_SPLIT_EVENTS <- file.path(DIR_QC, glue("{SCRIPT_ID}_split_events_latest.csv"))
FILE_SPLIT_SUMMARY<- file.path(DIR_QC, glue("{SCRIPT_ID}_split_summary_latest.csv"))
FILE_SAMPLE400    <- file.path(DIR_QC, glue("{SCRIPT_ID}_sample400_latest.csv"))

# ------------------------------------------------
# 2. Logging helpers 
# ------------------------------------------------
FILE_LOG_HELPERS <- here::here("R", "logger_helpers.R")
if (!file.exists(FILE_LOG_HELPERS)) stop("Missing logger helpers: ", FILE_LOG_HELPERS)
source(FILE_LOG_HELPERS)

log_meta <- start_log(SCRIPT_ID, DIR_LOGS)
on.exit(stop_log(log_meta$log_con), add = TRUE)

log_section(glue("{SCRIPT_ID} - START"))
log_info(glue("Project root: {DIR_ROOT}"))
log_info(glue("Input CSV:    {FILE_IN}"))

if (!file.exists(FILE_IN)) {
  log_error(glue("Missing input CSV: {FILE_IN}"))
  stop("Missing input CSV: ", FILE_IN)
}

# ------------------------------------------------
# 3. Load + validate
# ------------------------------------------------
dat <- with_timing("Read stable lineage CSV", quote({
  readr::read_csv(FILE_IN, show_col_types = FALSE) |>
    janitor::clean_names()
}))

req <- c("lineage_id", "year", "density_mean", "cbpseg", "quadid")
missing <- setdiff(req, names(dat))
if (length(missing) > 0) {
  log_error(glue("Missing required columns: {paste(missing, collapse = ', ')}"))
  stop("Missing required columns: ", paste(missing, collapse = ", "))
}

dat <- dat %>%
  mutate(
    year = suppressWarnings(as.integer(year)),
    lineage_id = suppressWarnings(as.integer(lineage_id))
  )

log_info(glue("Rows: {nrow(dat)}"))
log_info(glue("Unique lineages: {n_distinct(dat$lineage_id)}"))
log_info(glue("Year span: {min(dat$year, na.rm = TRUE)} - {max(dat$year, na.rm = TRUE)}"))

# ------------------------------------------------
# 4. Split detection
#   Split = same lineage_id appears >1x in the same year (fan-out)
# ------------------------------------------------
split_events <- dat %>%
  count(lineage_id, year, name = "n_patches") %>%
  filter(n_patches > 1) %>%
  arrange(desc(n_patches), lineage_id, year)

split_summary <- split_events %>%
  group_by(lineage_id) %>%
  summarise(
    n_years_with_split = n(),
    max_fanout_in_a_year = max(n_patches),
    first_split_year = min(year),
    last_split_year  = max(year),
    .groups = "drop"
  ) %>%
  arrange(desc(n_years_with_split), desc(max_fanout_in_a_year), lineage_id)

n_lineages_total <- n_distinct(dat$lineage_id)
n_lineages_split <- n_distinct(split_events$lineage_id)
pct_split <- ifelse(n_lineages_total == 0, NA_real_, 100 * n_lineages_split / n_lineages_total)

overall <- tibble::tibble(
  metric = c(
    "n_rows",
    "n_lineages_total",
    "year_min",
    "year_max",
    "n_years",
    "n_lineages_with_any_split",
    "pct_lineages_with_any_split",
    "max_fanout_observed"
  ),
  value = as.character(c(
    nrow(dat),
    n_lineages_total,
    min(dat$year, na.rm = TRUE),
    max(dat$year, na.rm = TRUE),
    n_distinct(dat$year),
    n_lineages_split,
    round(pct_split, 4),
    ifelse(nrow(split_events) == 0, 0, max(split_events$n_patches))
  ))
)

log_section("SPLIT SUMMARY")
log_info(glue("Lineages with any split: {n_lineages_split} of {n_lineages_total} ({round(pct_split, 3)}%)"))
log_info(glue("Max fanout observed: {ifelse(nrow(split_events) == 0, 0, max(split_events$n_patches))}"))

# ------------------------------------------------
# 5. Sample 400 lineage_ids for inspection
# ------------------------------------------------
set.seed(1)
pool <- sort(unique(dat$lineage_id))
sample_n <- min(400L, length(pool))
sample_ids <- sample(pool, size = sample_n, replace = FALSE)

sample_overview <- dat %>%
  filter(lineage_id %in% sample_ids) %>%
  group_by(lineage_id) %>%
  summarise(
    year_min = min(year, na.rm = TRUE),
    year_max = max(year, na.rm = TRUE),
    n_years  = n_distinct(year),
    n_rows   = n(),
    n_years_with_split = sum((count(cur_data(), year)$n > 1)),
    max_fanout = {
      tmp <- count(cur_data(), year)
      max(tmp$n)
    },
    .groups = "drop"
  ) %>%
  arrange(desc(n_years_with_split), desc(max_fanout), desc(n_years), lineage_id)

log_info(glue("Sampled {sample_n} lineages for inspection table."))

# ------------------------------------------------
# 6. Write outputs
# ------------------------------------------------
log_section("WRITE OUTPUTS")

readr::write_csv(overall, FILE_OVERALL)
readr::write_csv(split_events, FILE_SPLIT_EVENTS)
readr::write_csv(split_summary, FILE_SPLIT_SUMMARY)
readr::write_csv(sample_overview, FILE_SAMPLE400)

log_info(glue("Wrote: {FILE_OVERALL}"))
log_info(glue("Wrote: {FILE_SPLIT_EVENTS}"))
log_info(glue("Wrote: {FILE_SPLIT_SUMMARY}"))
log_info(glue("Wrote: {FILE_SAMPLE400}"))

log_section("DONE")
log_info("Stage 0b split-topology check complete.")

