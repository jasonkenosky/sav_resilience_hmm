# ==============================================================================
# Script Title: n4_01_fit_hmms_and_export_states
# Purpose: Fit HMM(s) to Stage0 observed state trajectories and export:
#          (1) model selection diagnostics, (2) per-year HMM state assignments,
#          (3) per-lineage regime summaries (occupancy/switching/dwell)
# Author: Jason Kenosky
# Last Updated: 2025-12-16
#
# Inputs (from n4_00_prep_hmm_inputs):
#   outputs/hmm/n4_00_prep_hmm_inputs_hmm_input_long_latest.csv
#
# Outputs:
#   outputs/hmm/n4_01_fit_hmms_model_selection_latest.csv
#   outputs/hmm/n4_01_fit_hmms_states_long_latest.csv
#   outputs/hmm/n4_01_fit_hmms_states_long_<timestamp>.csv
#   outputs/metrics/n4_01_regimes_hmm_summaries_metrics_latest.csv
#   outputs/metrics/n4_01_regimes_hmm_summaries_metrics_<timestamp>.csv
#   outputs/qc/n4_01_fit_hmms_and_export_states_qc_latest.csv
#   outputs/qc/n4_01_fit_hmms_and_export_states_qc_<timestamp>.csv
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
  library(tidyr) 
  library(depmixS4)   
})

# ------------------------------------------------
# 2. Script configuration
# ------------------------------------------------
SCRIPT_ID <- "n4_01_fit_hmms_and_export_states"

DIR_ROOT    <- here::here()
DIR_OUTPUTS <- file.path(DIR_ROOT, "outputs")
DIR_LOGS    <- file.path(DIR_OUTPUTS, "logs")
DIR_QC      <- file.path(DIR_OUTPUTS, "qc")
DIR_HMM     <- file.path(DIR_OUTPUTS, "hmm")
DIR_METRICS <- file.path(DIR_OUTPUTS, "metrics")

fs::dir_create(DIR_LOGS, recurse = TRUE)
fs::dir_create(DIR_QC, recurse = TRUE)
fs::dir_create(DIR_HMM, recurse = TRUE)
fs::dir_create(DIR_METRICS, recurse = TRUE)

timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")

FILE_IN_LONG <- file.path(DIR_HMM, "n4_00_prep_hmm_inputs_hmm_input_long_latest.csv")

FILE_OUT_SEL_LATEST   <- file.path(DIR_HMM, glue("n4_01_fit_hmms_model_selection_latest.csv"))
FILE_OUT_SEL_RUN      <- file.path(DIR_HMM, glue("n4_01_fit_hmms_model_selection_{timestamp}.csv"))

FILE_OUT_STATES_LATEST <- file.path(DIR_HMM, glue("n4_01_fit_hmms_states_long_latest.csv"))
FILE_OUT_STATES_RUN    <- file.path(DIR_HMM, glue("n4_01_fit_hmms_states_long_{timestamp}.csv"))

FILE_OUT_MET_LATEST <- file.path(DIR_METRICS, glue("n4_01_regimes_hmm_summaries_metrics_latest.csv"))
FILE_OUT_MET_RUN    <- file.path(DIR_METRICS, glue("n4_01_regimes_hmm_summaries_metrics_{timestamp}.csv"))

FILE_QC_LATEST <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_latest.csv"))
FILE_QC_RUN    <- file.path(DIR_QC, glue("{SCRIPT_ID}_qc_{timestamp}.csv"))

PARAMS <- list(
  required_cols = c("lineage_id", "year", "state_bin7"),
  allowed_states = as.character(c(1, 1.5, 2, 2.5, 3, 3.5, 4)),
  
  # HMM grid
  K_grid = c(2, 3, 4),
  
  # If TRUE, only fit on observed rows (state_bin7 not NA).
  # You can still export a full-year table with NA hmm_state for gaps.
  fit_drop_na_states = TRUE,
  
  # Reproducibility
  seed = 123
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
  inputs = list("HMM input long" = FILE_IN_LONG),
  outputs = list(
    "Model selection (latest)" = FILE_OUT_SEL_LATEST,
    "Model selection (run)"    = FILE_OUT_SEL_RUN,
    "HMM states long (latest)" = FILE_OUT_STATES_LATEST,
    "HMM states long (run)"    = FILE_OUT_STATES_RUN,
    "Regime metrics (latest)"  = FILE_OUT_MET_LATEST,
    "Regime metrics (run)"     = FILE_OUT_MET_RUN,
    "QC (latest)"              = FILE_QC_LATEST,
    "QC (run)"                 = FILE_QC_RUN
  )
)
log_params(PARAMS)

# ------------------------------------------------
# 4. Read + validate
# ------------------------------------------------
log_section("READ + VALIDATE INPUT")

stopifnot(file.exists(FILE_IN_LONG))

dat0 <- with_timing("Read HMM input long", quote({
  readr::read_csv(FILE_IN_LONG, show_col_types = FALSE) |>
    janitor::clean_names()
}))

check_required_cols(dat0, PARAMS$required_cols, context = "HMM input long")

# Enforce types + canonical ordering
dat0 <- dat0 %>%
  transmute(
    lineage_id = suppressWarnings(as.integer(lineage_id)),
    year       = suppressWarnings(as.integer(year)),
    state_bin7 = as.character(state_bin7),
    cbpseg     = if ("cbpseg" %in% names(dat0)) as.character(dat0$cbpseg) else NA_character_,
    quadid     = if ("quadid" %in% names(dat0)) as.character(dat0$quadid) else NA_character_,
    state      = if ("state"  %in% names(dat0)) as.character(dat0$state)  else NA_character_,
    zone       = if ("zone"   %in% names(dat0)) as.character(dat0$zone)   else NA_character_
  ) %>%
  arrange(lineage_id, year)

# Deduplicate (lineage_id, year) deterministically
dat0 <- dat0 %>%
  group_by(lineage_id, year) %>%
  summarise(
    state_bin7 = dplyr::first(state_bin7),
    cbpseg     = dplyr::first(cbpseg),
    quadid     = dplyr::first(quadid),
    state      = dplyr::first(state),
    zone       = dplyr::first(zone),
    .groups    = "drop"
  ) %>%
  arrange(lineage_id, year)

log_info(glue("Rows: {nrow(dat0)} | Lineages: {n_distinct(dat0$lineage_id)} | Years: {n_distinct(dat0$year)}"))
log_info(glue("Year span: {min(dat0$year, na.rm = TRUE)}-{max(dat0$year, na.rm = TRUE)}"))

# Validate allowed alphabet where non-missing
bad_states <- dat0 %>%
  filter(!is.na(state_bin7) & !(state_bin7 %in% PARAMS$allowed_states)) %>%
  count(state_bin7, name = "n") %>%
  arrange(desc(n))

if (nrow(bad_states) > 0) {
  log_error(glue("Found states outside allowed alphabet: {paste(bad_states$state_bin7, collapse = ', ')}"))
  stop("state_bin7 contains values outside allowed_states. See log.")
}

# depmixS4 needs a factor for multinomial emissions
dat_fit <- dat0
if (isTRUE(PARAMS$fit_drop_na_states)) {
  dat_fit <- dat_fit %>% filter(!is.na(state_bin7))
}

dat_fit <- dat_fit %>%
  mutate(
    state_obs = factor(state_bin7, levels = PARAMS$allowed_states),
    # depmix expects 'ntimes' as segment lengths; we use one segment per lineage
    lineage_id = as.integer(lineage_id)
  ) %>%
  arrange(lineage_id, year)

# Segment lengths (one per lineage, in current ordering)
ntimes <- dat_fit %>%
  count(lineage_id, name = "n") %>%
  arrange(lineage_id) %>%
  pull(n)

stopifnot(sum(ntimes) == nrow(dat_fit))

# ------------------------------------------------
# 5. Helpers
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

check_required_cols <- function(df, required_cols, context = "data") {
  missing <- setdiff(required_cols, names(df))
  if (length(missing) > 0) {
    msg <- glue("Missing required columns in {context}: {paste(missing, collapse = ', ')}")
    log_error(msg)
    stop(msg)
  }
  invisible(TRUE)
}

# Computes run lengths for an integer state vector (NAs already removed)
run_lengths <- function(x) {
  r <- rle(x)
  tibble::tibble(state = r$values, run_len = r$lengths)
}


# ------------------------------------------------
# 6. Main process: Fit HMMs (K grid) + model selection
# ------------------------------------------------
log_section("FIT HMMS + MODEL SELECTION")

set.seed(PARAMS$seed)

fit_one <- function(K, dat_fit, ntimes) {
  # Multinomial emission on observed states, intercept-only
  mod <- depmixS4::depmix(
    response = state_obs ~ 1,
    data     = dat_fit,
    nstates  = K,
    family   = multinomial(),
    ntimes   = ntimes
  )
  
  fit <- tryCatch(
    depmixS4::fit(mod, verbose = FALSE),
    error = function(e) {
      log_warn(glue("Fit failed for K={K}: {conditionMessage(e)}"))
      NULL
    }
  )
  fit
}

fits <- list()
sel_rows <- list()

for (K in PARAMS$K_grid) {
  log_section(glue("FIT K = {K}"))
  
  fitK <- with_timing(
    glue("Fit HMM (K={K})"),
    quote({
      fit_one(K, dat_fit, ntimes)
    })
  )
  
  fits[[as.character(K)]] <- fitK
  
  if (is.null(fitK)) {
    sel_rows[[length(sel_rows) + 1L]] <- tibble(
      K = K, converged = FALSE,
      logLik = NA_real_, AIC = NA_real_, BIC = NA_real_,
      n_obs = nrow(dat_fit),
      n_lineages = n_distinct(dat_fit$lineage_id),
      note = "fit_failed"
    )
  } else {
    sel_rows[[length(sel_rows) + 1L]] <- tibble(
      K = K, converged = TRUE,
      logLik = as.numeric(logLik(fitK)),
      AIC = AIC(fitK),
      BIC = BIC(fitK),
      n_obs = nrow(dat_fit),
      n_lineages = n_distinct(dat_fit$lineage_id),
      note = NA_character_
    )
  }
}

model_sel <- bind_rows(sel_rows) %>% arrange(K)

# Pick best by BIC (conservative)
best_row <- model_sel %>%
  filter(converged) %>%
  arrange(BIC) %>%
  slice(1)

if (nrow(best_row) == 0) stop("No HMM fits converged across K grid.")

K_best <- best_row$K[[1]]
fit_best <- fits[[as.character(K_best)]]

log_info(glue("Best model by BIC: K={K_best} | BIC={best_row$BIC[[1]]} | AIC={best_row$AIC[[1]]}"))

# ------------------------------------------------
# 6b. Export HMM state assignments (best model)
# ------------------------------------------------
log_section("EXPORT HMM STATES (BEST K)")

# posterior() returns per-observation posteriors and most likely state ("state")
post <- with_timing("Compute posterior (best model)", quote({
  depmixS4::posterior(fit_best, type = "viterbi")
}))

# Align posterior rows to dat_fit ordering
stopifnot(nrow(post) == nrow(dat_fit))

states_long_fit <- dat_fit %>%
  transmute(
    lineage_id,
    year,
    state_bin7,
    hmm_K = K_best,
    hmm_state = as.integer(post$state)
  )

# Merge back onto full dat0 (so gaps remain, hmm_state = NA)
states_long_full <- dat0 %>%
  dplyr::left_join(
    states_long_fit %>% dplyr::select(lineage_id, year, hmm_K, hmm_state),
    by = c("lineage_id", "year")
  ) %>%
  dplyr::mutate(hmm_K = K_best) %>%   # stamp best K for all rows
  dplyr::arrange(lineage_id, year)

log_info(glue("States long rows: {nrow(states_long_full)} | with hmm_state: {sum(!is.na(states_long_full$hmm_state))}"))

# ------------------------------------------------
# 6c. Regime metrics per lineage (best model)
# ------------------------------------------------
log_section("COMPUTE REGIME METRICS (PER LINEAGE)")

metrics <- with_timing("Compute per-lineage regime summaries", quote({
  
  base <- states_long_fit %>%
    dplyr::group_by(lineage_id) %>%
    dplyr::summarise(
      n_obs_years = dplyr::n(),
      year_min    = min(year, na.rm = TRUE),
      year_max    = max(year, na.rm = TRUE),
      span_years  = year_max - year_min + 1L,
      hmm_K       = dplyr::first(hmm_K),
      
      n_switches = sum(dplyr::lag(hmm_state) != hmm_state, na.rm = TRUE),
      switches_per_step = dplyr::if_else(n_obs_years > 1,
                                         n_switches / (n_obs_years - 1),
                                         NA_real_),
      
      # run-lengths (compute once per lineage)
      n_runs = {
        rl <- run_lengths(hmm_state)
        nrow(rl)
      },
      median_dwell = {
        rl <- run_lengths(hmm_state)
        stats::median(rl$run_len)
      },
      mean_dwell = {
        rl <- run_lengths(hmm_state)
        base::mean(rl$run_len)
      },
      max_dwell = {
        rl <- run_lengths(hmm_state)
        max(rl$run_len)
      },
      
      .groups = "drop"
    )
  
  occ <- states_long_fit %>%
    dplyr::count(lineage_id, hmm_state, name = "n") %>%
    dplyr::group_by(lineage_id) %>%
    dplyr::mutate(p = n / sum(n)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(metric = glue::glue("p_state_{hmm_state}")) %>%
    dplyr::select(lineage_id, metric, p) %>%
    tidyr::pivot_wider(
      names_from = metric,
      values_from = p,
      values_fill = 0
    )
  
  base %>% dplyr::left_join(occ, by = "lineage_id")
}))
log_info(glue("Metrics rows: {nrow(metrics)} (should equal n_lineages in fit set)"))

# ------------------------------------------------
# 7. Write outputs
# ------------------------------------------------
log_section("WRITE OUTPUTS")

qc <- tibble::tibble()

qc <- bind_rows(
  qc,
  qc_row("input_rows_total", nrow(dat0)),
  qc_row("input_rows_fit", nrow(dat_fit)),
  qc_row("n_lineages_total", n_distinct(dat0$lineage_id)),
  qc_row("n_lineages_fit", n_distinct(dat_fit$lineage_id)),
  qc_row("K_best", K_best),
  qc_row("best_BIC", best_row$BIC[[1]]),
  qc_row("best_AIC", best_row$AIC[[1]])
)

with_timing("Write model selection, states, metrics, QC", quote({
  safe_write_csv(model_sel, FILE_OUT_SEL_LATEST)
  safe_write_csv(model_sel, FILE_OUT_SEL_RUN)
  
  safe_write_csv(states_long_full, FILE_OUT_STATES_LATEST)
  safe_write_csv(states_long_full, FILE_OUT_STATES_RUN)
  
  if (exists("metrics")) {
    safe_write_csv(metrics, FILE_OUT_MET_LATEST)
    safe_write_csv(metrics, FILE_OUT_MET_RUN)
  } else {
    log_warn("metrics object does not exist; skipping metrics write.")
  }
  
  safe_write_csv(qc, FILE_QC_LATEST)
  safe_write_csv(qc, FILE_QC_RUN)
}))

log_info(glue("Selection latest: {FILE_OUT_SEL_LATEST}"))
log_info(glue("States latest:    {FILE_OUT_STATES_LATEST}"))
log_info(glue("Metrics latest:   {FILE_OUT_MET_LATEST}"))
log_info(glue("QC latest:        {FILE_QC_LATEST}"))

# ------------------------------------------------
# 8. QC summary (always write)
# ------------------------------------------------

# ------------------------------------------------
# 9. Done
# ------------------------------------------------
log_section("DONE")
log_info("N4 HMM fit + exports completed successfully.")
