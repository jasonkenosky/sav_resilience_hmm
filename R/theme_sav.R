#===================================================================
#R/theme_sav.R
#
# Core plotting theme + palettes for md_sav_dynamics.
# Goals:
#   - Colorblind-safe by default
#   - Sans-serif base font
#   - Centered titles/subtitles
#   - Legend off by default (figures must stand on their own)
#
# Usage:
#   ggplot(...) +
#     geom_...() +
#     theme_sav() +
#     scale_color_sav_state("area_state")  # example
#===================================================================

suppressPackageStartupMessages({
  library(ggplot2)
})

# -------------------------------------------------------------------
# 1. Base theme
# -------------------------------------------------------------------

theme_sav <- function(base_size = 11,
                      base_family = "sans",
                      legend_position = "none") {
  ggplot2::theme_minimal(base_size = base_size, base_family = base_family) +
    theme(
      # Titles centered
      plot.title = element_text(
        hjust = 0.5,
        face  = "bold"
      ),
      plot.subtitle = element_text(
        hjust = 0.5
      ),
      
      # Axes
      axis.title.x = element_text(margin = margin(t = 6)),
      axis.title.y = element_text(margin = margin(r = 6)),
      axis.text = element_text(size = rel(0.9)),
      
      # Panel grid: keep light, no major clutter
      panel.grid.major = element_line(linewidth = 0.3),
      panel.grid.minor = element_blank(),
      
      # Legend – off by default; override in script if needed
      legend.position = legend_position,
      legend.title    = element_text(face = "bold"),
      legend.key      = element_blank(),
      
      # Strip text for facets
      strip.text = element_text(face = "bold")
    )
}

# Convenience wrappers if you *know* you want legend on the right/bottom:
theme_sav_legend_right <- function(...) {
  theme_sav(legend_position = "right", ...)
}

theme_sav_legend_bottom <- function(...) {
  theme_sav(legend_position = "bottom", ...)
}

# -------------------------------------------------------------------
# 2. Colorblind-safe palettes
#    (Okabe–Ito palette variants)
# -------------------------------------------------------------------

# Okabe–Ito base colors (for reference):
# black       #000000
# orange      #E69F00
# sky blue    #56B4E9
# bluish green#009E73
# yellow      #F0E442
# blue        #0072B2
# vermillion  #D55E00
# reddish purp#CC79A7

# States for area / density regime plots
sav_palette_states <- c(
  "Gain"     = "#0072B2",  # blue
  "Loss"     = "#D55E00",  # vermillion
  "Stable"   = "#009E73",  # bluish green
  "Absent"   = "#999999",  # neutral grey
  "Missing"  = "#E69F00"   # orange
)

# Density classes (1–4) – low to high
sav_palette_density <- c(
  "1" = "#999999",  # very sparse
  "2" = "#56B4E9",  # low/moderate
  "3" = "#0072B2",  # moderate/high
  "4" = "#009E73"   # dense
)

# Regions (Eastern vs Western vs “Other”)
sav_palette_region <- c(
  "Eastern Shore" = "#0072B2",
  "Western Shore" = "#D55E00",
  "Other"         = "#009E73"
)

# -------------------------------------------------------------------
# 3. Scale helpers
# -------------------------------------------------------------------

# For area_state / dens_state variables:
scale_color_sav_state <- function(na.value = "grey80", ...) {
  scale_color_manual(
    values   = sav_palette_states,
    drop     = FALSE,
    na.value = na.value,
    ...
  )
}

scale_fill_sav_state <- function(na.value = "grey80", ...) {
  scale_fill_manual(
    values   = sav_palette_states,
    drop     = FALSE,
    na.value = na.value,
    ...
  )
}

# For density classes:
scale_color_sav_density <- function(na.value = "grey80", ...) {
  scale_color_manual(
    values   = sav_palette_density,
    drop     = FALSE,
    na.value = na.value,
    ...
  )
}

scale_fill_sav_density <- function(na.value = "grey80", ...) {
  scale_fill_manual(
    values   = sav_palette_density,
    drop     = FALSE,
    na.value = na.value,
    ...
  )
}

# For regions:
scale_color_sav_region <- function(na.value = "grey80", ...) {
  scale_color_manual(
    values   = sav_palette_region,
    drop     = FALSE,
    na.value = na.value,
    ...
  )
}

scale_fill_sav_region <- function(na.value = "grey80", ...) {
  scale_fill_manual(
    values   = sav_palette_region,
    drop     = FALSE,
    na.value = na.value,
    ...
  )
}