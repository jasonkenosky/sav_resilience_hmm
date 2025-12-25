# sav_resilience_hmm

**Working title:**  
Resilience signals in Chesapeake Bay SAV from HMM lineage dynamics (1984–2023)

---

[![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.XXXXXXX.svg)](https://doi.org/10.5281/zenodo.18055868)


## Overview

This project evaluates long-term resilience properties of Submerged Aquatic Vegetation (SAV) in the Chesapeake Bay using **Hidden Markov Model (HMM) hidden states** derived from lineage-based tracking of VIMS SAV datasets spanning approximately 40 years.

The analysis is grounded in **resilience theory** and focuses on a small, explicitly defined set of metrics derived from SAV state dynamics. The project is lineage-aware, split/merge aware, and designed to be fully reproducible.

This repository contains analysis scripts, helper utilities, documentation, and selected outputs. Raw VIMS SAV data are not redistributed.

---

## Conceptual scope

The project quantifies five resilience-theory properties:

1. **Identity (Persistence)**  
   The tendency of a lineage to maintain its state over time.

2. **Memory (Autocorrelation)**  
   The degree to which prior states constrain subsequent states.

3. **Stability (Variance / Volatility)**  
   The frequency and magnitude of state changes.

4. **Nonlinearity (Regimes)**  
   The presence of discrete modes of behavior represented by HMM hidden states and their switching dynamics.

5. **Cross-scale buffering (Synchrony)**  
   The degree to which state dynamics are synchronized or decoupled across spatial scales.

Optional (secondary, if required): **Shannon entropy** of HMM state occupancy.

### Explicit exclusions

This project does **not** include:
- composite “resilience indices”
- transfer entropy or directional information flow
- Lyapunov exponents or chaos metrics
- large catalogs of patch metrics
- policy evaluation or management optimization

---

## Folder structure

```text
sav_resilience_hmm/
├── data/
│   ├── data_raw/
│   └── data_processed/
├── scripts/
├── R/
├── docs/
├── outputs/
│   ├── figs/
│   ├── tables/
│   ├── qc/
│   └── logs/
├── README.md
└── PROJECT_NOTES.md
```

### Directory roles

- `data/data_raw/`  
  Original or externally sourced data. Read-only.

- `data/data_processed/`  
  Canonical derived datasets used by analysis scripts.

- `scripts/`  
  Analysis scripts. One conceptual task per script.

- `R/`  
  Shared helper functions (logging, validation, QC).

- `docs/`  
  Project documentation, figures-in-progress, conceptual notes, and method sketches that are not scripts.

- `outputs/`  
  Final products only.
  - `figs/`: publication-ready figures
  - `tables/`: final tables
  - `qc/`: quality-control summaries
  - `logs/`: execution logs

---

## Primary inputs

Canonical lineage datasets located in:

`data/data_processed/`

Expected files:
- `sav_lineages_hmm.gpkg`
- `sav_lineages_hmm.csv`

### Required columns
- `lineage_id`
- `year`
- `hmm_state`
- `cbpseg`
- `quadid`

Additional lineage metadata (e.g., split/merge indicators, parent-child IDs) are preserved if present.

---

## Canonical derived table

All downstream scripts operate on a single long-format table:

`lineage_id × year × hmm_state × cbpseg × quadid`

Saved as:

`data/data_processed/lineage_timeseries_hmm.csv`

This table is the only required dependency for metric scripts.

---

## Spatial scale for cross-scale buffering

Cross-scale buffering and synchrony are evaluated using:

- **CBPSEG** — environmentally meaningful management units
- **QUADID** — uniform spatial units for geometry-based scaling

Region and county are explicitly excluded to avoid mixing patch-tracking versions.

---

## Lineage structure and sequencing rule

Lineages may branch due to split and merge events and are not assumed to be simple chains.

Before freezing sequencing rules for metric calculation, the project generates:
- lineage graph representations
- summaries of split, merge, continuation, and termination events

The final rule (e.g., trunk-based, graph-based, or segment-based metrics) is documented in `PROJECT_NOTES.md` before metric scripts are run.

---

## Script plan

**Setup and validation**
- `n0_01_validate_inputs`
- `n0_02_build_lineage_timeseries_table`
- `n0_03_build_lineage_graph_qc`

**Metrics**
- `n1_01_identity_persistence`
- `n2_01_memory_autocorrelation`
- `n3_01_stability_variance`
- `n4_01_regimes_hmm`
- `n5_01_synchrony_cross_scale`
- `n6_01_entropy_optional` (only if invoked)

**Compilation**
- `n7_01_compile_resilience_metrics`

Each script:
- follows the frozen script skeleton
- validates inputs explicitly
- logs parameters and session info
- writes QC outputs and logs
- produces outputs usable by subsequent scripts

---

## Reproducibility

- All scripts use `here::here()`; no `setwd()`
- Package versions are locked and recorded
- Raw data are never overwritten
- Missing data are preserved and treated as information
- Logs and QC summaries are written for every script run

---

## Version control

This repository includes:
- scripts
- helpers
- documentation
- selected final tables and figures

Raw VIMS SAV data and large intermediates are excluded via `.gitignore`.