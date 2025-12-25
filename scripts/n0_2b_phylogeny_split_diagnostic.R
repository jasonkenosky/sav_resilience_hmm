# QC artifact.  Diagnostic purposes only
# n0_2b_phylogeny_split_diagnostic.R

library(readr)
library(dplyr)

ss <- readr::read_csv(
  "outputs/qc/n0_02_phylogeny_split_check_split_summary_latest.csv",
  show_col_types = FALSE
)

# --- terminal year (do NOT hard-code 2023) ---
max_year <- max(ss$last_split_year, na.rm = TRUE)

# --- exclude right-censored splits: first_split_year >= (max_year - 1) ---
ss_filt <- ss %>%
  filter(first_split_year < (max_year - 1))

# --- summary on full set (including right-censored) ---
summary_all <- ss %>%
  summarise(
    dataset_max_year = max_year,
    n_lineages_with_splits = n(),
    median_years_with_split = median(n_years_with_split, na.rm = TRUE),
    p90_years_with_split = quantile(n_years_with_split, 0.90, na.rm = TRUE),
    max_years_with_split = max(n_years_with_split, na.rm = TRUE),
    median_max_fanout = median(max_fanout_in_a_year, na.rm = TRUE),
    p90_max_fanout = quantile(max_fanout_in_a_year, 0.90, na.rm = TRUE),
    max_fanout = max(max_fanout_in_a_year, na.rm = TRUE)
  )

# --- summary after excluding right-censored ---
summary_uncensored <- ss_filt %>%
  summarise(
    dataset_max_year = max_year,
    n_lineages_with_splits = n(),
    median_years_with_split = median(n_years_with_split, na.rm = TRUE),
    p90_years_with_split = quantile(n_years_with_split, 0.90, na.rm = TRUE),
    max_years_with_split = max(n_years_with_split, na.rm = TRUE),
    median_max_fanout = median(max_fanout_in_a_year, na.rm = TRUE),
    p90_max_fanout = quantile(max_fanout_in_a_year, 0.90, na.rm = TRUE),
    max_fanout = max(max_fanout_in_a_year, na.rm = TRUE)
  )

# --- how many did we exclude? ---
excluded <- ss %>% filter(first_split_year >= (max_year - 1))

excluded_counts <- excluded %>%
  count(first_split_year, name = "n") %>%
  arrange(first_split_year)

print(summary_all)
print(summary_uncensored)
print(excluded_counts)
