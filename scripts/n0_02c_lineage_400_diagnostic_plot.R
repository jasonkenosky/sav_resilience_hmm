# Quick diagnostic plot for a single lineage
# n0_02c_lineage_400_diagnostic_plot.R

library(readr)
library(dplyr)
library(igraph)

edges <- read_csv(
  "data/data_processed/lineages/sav_superpatch_lineage_links_stable.csv",
  show_col_types = FALSE
) %>%
  filter(from_lineage_id == 400 | to_lineage_id == 400) %>%
  transmute(
    from = paste(from_year, from_fid, sep = ":"),
    to   = paste(to_year,   to_fid,   sep = ":")
  ) %>%
  distinct()

g <- graph_from_data_frame(edges, directed = TRUE)

#--- "Start" nodes in this subgraph: no incoming edges ---
start_nodes <- V(g)[degree(g, mode = "in") == 0]

V(g)$color <- "grey70"
V(g)$size  <- 4
V(g)$label <- NA

#--- Highlight the starting patch ---
V(g)[start_nodes]$color <- "#D55E00"
V(g)[start_nodes]$size  <- 7

plot(
  g,
  edge.arrow.size = 0.3,
  main = "Patch lineage flow (lineage_id = 400)"
)

#--- Add a tiny legend ---
legend(
  "topleft",
  legend = c("start patch", "other patches"),
  pch = 19,
  col = c("#D55E00", "grey70"),
  pt.cex = c(1.4, 1.0),
  bty = "n"
)

#------------------------------------
# Min year start version
#------------------------------------

edges <- read_csv(
  "data/data_processed/lineages/sav_superpatch_lineage_links_stable.csv",
  show_col_types = FALSE
) %>%
  filter(from_lineage_id == 400 | to_lineage_id == 400) %>%
  transmute(
    from = paste(from_year, from_fid, sep = ":"),
    to   = paste(to_year,   to_fid,   sep = ":")
  ) %>%
  distinct()

g <- graph_from_data_frame(edges, directed = TRUE)

#--- Parse year from vertex names like "1984:123" ---
v_names <- V(g)$name
v_year  <- suppressWarnings(as.integer(sub(":.*$", "", v_names)))

min_year <- min(v_year, na.rm = TRUE)

#--- If multiple nodes exist in the min year, highlight all of them ---
start_nodes <- V(g)[!is.na(v_year) & v_year == min_year]

V(g)$color <- "grey70"
V(g)$size  <- 4
V(g)$label <- NA

V(g)[start_nodes]$color <- "#D55E00"
V(g)[start_nodes]$size  <- 7

plot(
  g,
  edge.arrow.size = 0.3,
  main = paste0("Patch lineage flow (lineage_id = 400) | start year = ", min_year)
)

legend(
  "topleft",
  legend = c(paste0("start year (", min_year, ")"), "other patches"),
  pch = 19,
  col = c("#D55E00", "grey70"),
  pt.cex = c(1.4, 1.0),
  bty = "n"
)
