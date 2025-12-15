# ============================================================
# File: logger_helpers.R
# Purpose: Reusable logging utilities for reproducible R scripts
# Author: Jason Kenosky
# Philosophy: clarity > cleverness | nouns for objects | verbs for functions
# ============================================================

suppressPackageStartupMessages({
  library(glue)
  library(tibble)
  library(readr)
  library(dplyr)
})

# ---- Internal helpers -------------------------------------------------------

.safe_cat <- function(...) {
  # Never throw from logging (prevents recursive wrapup)
  tryCatch(cat(...), error = function(e) invisible(NULL))
}

.safe_sink_off <- function(type = c("output", "message")) {
  type <- match.arg(type)
  tryCatch({
    while (sink.number(type = type) > 0) sink(NULL, type = type)
  }, error = function(e) invisible(NULL))
}

.safe_close <- function(con) {
  tryCatch({
    if (!is.null(con) && inherits(con, "connection") && isOpen(con)) close(con)
  }, error = function(e) invisible(NULL))
}

# ---- Create log file and sinks ---------------------------------------------

start_log <- function(script_id, dir_logs) {
  timestamp <- format(Sys.time(), "%Y%m%d-%H%M%S")
  log_file  <- file.path(dir_logs, glue("{script_id}_{timestamp}.log"))
  
  dir.create(dir_logs, showWarnings = FALSE, recursive = TRUE)
  
  # open connection
  log_con <- file(log_file, open = "wt")
  
  # establish sinks (guarded)
  tryCatch(sink(log_con, type = "output"),  error = function(e) invisible(NULL))
  tryCatch(sink(log_con, type = "message"), error = function(e) invisible(NULL))
  
  .safe_cat("--------------------------------------------------\n")
  .safe_cat(glue("Script:      {script_id}\n"))
  .safe_cat(glue("Started:     {Sys.time()}\n"))
  .safe_cat(glue("Log file:    {log_file}\n"))
  .safe_cat(glue("Working dir: {getwd()}\n"))
  .safe_cat("--------------------------------------------------\n\n")
  
  list(log_file = log_file, log_con = log_con)
}

# ---- Clean-up on exit ------------------------------------------------------

stop_log <- function(log_con) {
  # Never error here. Ever.
  .safe_cat("\n--------------------------------------------------\n")
  .safe_cat(glue("Finished:    {Sys.time()}\n"))
  .safe_cat("--------------------------------------------------\n")
  
  # Turn off sinks safely (in case they were never set, or already closed)
  .safe_sink_off("message")
  .safe_sink_off("output")
  
  # Close connection safely
  .safe_close(log_con)
  invisible(TRUE)
}

# ---- System info ----------------------------------------------------------

log_system_info <- function() {
  
  cat("\n")
  cat("--------------------------------------------------\n")
  cat("System information\n")
  cat("--------------------------------------------------\n")
  
  cat(glue("R version:        {R.version.string}\n"))
  cat(glue("Platform:         {Sys.info()[['sysname']]} {Sys.info()[['release']]}\n"))
  cat(glue("Machine:          {Sys.info()[['machine']]}\n"))
  cat(glue("Architecture:     {R.version[['arch']]}\n"))
  cat(glue("User:             {Sys.info()[['user']]}\n"))
  cat(glue("Locale:           {Sys.getlocale()}\n"))
  cat(glue("Time zone:        {Sys.timezone()}\n"))
  cat(glue("Working directory:{getwd()}\n"))
  
  # Memory (best-effort, platform-aware)
  mem <- tryCatch({
    gc_out <- gc()
    round(sum(gc_out[, "used"]) / 1024, 2)
  }, error = function(e) NA_real_)
  
  if (!is.na(mem)) {
    cat(glue("R memory used:    {mem} MB\n"))
  }
  
  cat("--------------------------------------------------\n\n")
}

# ---- Logging utilities -----------------------------------------------------

log_section <- function(title) {
  .safe_cat("\n\n")
  .safe_cat("==================================================\n")
  .safe_cat(glue("{title}\n"))
  .safe_cat("==================================================\n")
}

log_info <- function(msg, ...)  .safe_cat(glue("[INFO  {Sys.time()}] {msg}", ...), "\n")
log_warn <- function(msg, ...)  .safe_cat(glue("[WARN  {Sys.time()}] {msg}", ...), "\n")
log_error <- function(msg, ...) .safe_cat(glue("[ERROR {Sys.time()}] {msg}", ...), "\n")

log_memory <- function(label = NULL) {
  mem <- tryCatch(sum(gc()[,"used"]) / 1024, error = function(e) NA_real_)
  .safe_cat(glue("[MEM   {Sys.time()}] {label} | {round(mem, 2)} MB"), "\n")
}


# ---- Capture packages ------------------------------------------------------

log_packages <- function() {
  pkgs <- sort(.packages())
  
  tbl <- tibble::tibble(
    package = pkgs,
    version = vapply(pkgs, function(p) {
      tryCatch(as.character(utils::packageVersion(p)), error = function(e) NA)
    }, character(1))
  )
  
  cat("\n")
  cat("--------------------------------------------------\n")
  cat("Loaded R packages\n")
  cat("--------------------------------------------------\n")
  
  apply(tbl, 1, function(row) {
    cat(sprintf("  - %-20s %s\n", row["package"], row["version"]))
  })
  
  cat("--------------------------------------------------\n\n")
}

# ---- Timing helper ---------------------------------------------------------

with_timing <- function(label, expr) {
  start_time <- Sys.time()
  log_info(glue("Starting: {label}"))
  
  result <- tryCatch({
    if (is.function(expr)) {
      expr()
    } else {
      # capture and eval in parent frame
      eval(substitute(expr), envir = parent.frame())
    }
  }, error = function(e) {
    log_error(glue("Error in {label}: {e$message}"))
    stop(e)
  })
  
  elapsed <- round(as.numeric(Sys.time() - start_time, units = "secs"), 2)
  log_info(glue("Completed: {label} ({elapsed} sec)"))
  log_memory(label)
  
  result
}

# ---- Task map / intent logging ---------------------------------------------

log_task_map <- function(tasks) {
  # tasks: named character vector or list
  # e.g. list("Geometry ops"="sf", "Graph components"="igraph")
  cat("\n")
  cat("--------------------------------------------------\n")
  cat("Task map (what is doing what)\n")
  cat("--------------------------------------------------\n")
  
  if (is.null(tasks) || length(tasks) == 0) {
    cat("  (none provided)\n")
  } else {
    nms <- names(tasks)
    for (i in seq_along(tasks)) {
      task <- if (!is.null(nms) && nzchar(nms[i])) nms[i] else paste0("Task ", i)
      who  <- as.character(tasks[[i]])
      cat(sprintf("  - %-28s %s\n", task, who))
    }
  }
  
  cat("--------------------------------------------------\n\n")
}

log_inputs_outputs <- function(inputs = NULL, outputs = NULL) {
  cat("\n")
  cat("--------------------------------------------------\n")
  cat("Inputs / Outputs\n")
  cat("--------------------------------------------------\n")
  
  if (!is.null(inputs) && length(inputs) > 0) {
    cat("Inputs:\n")
    for (nm in names(inputs)) cat(glue("  - {nm}: {inputs[[nm]]}\n"))
  } else {
    cat("Inputs:\n  (none listed)\n")
  }
  
  cat("\n")
  
  if (!is.null(outputs) && length(outputs) > 0) {
    cat("Outputs:\n")
    for (nm in names(outputs)) cat(glue("  - {nm}: {outputs[[nm]]}\n"))
  } else {
    cat("Outputs:\n  (none listed)\n")
  }
  
  cat("--------------------------------------------------\n\n")
}

log_params <- function(params) {
  cat("\n")
  cat("--------------------------------------------------\n")
  cat("Parameters\n")
  cat("--------------------------------------------------\n")
  
  if (is.null(params) || length(params) == 0) {
    cat("  (none provided)\n")
  } else {
    for (nm in names(params)) {
      val <- params[[nm]]
      if (length(val) > 1) val <- paste(val, collapse = ", ")
      cat(glue("  - {nm}: {val}\n"))
    }
  }
  
  cat("--------------------------------------------------\n\n")
}

# ---- Library loader (optional) ---------------------------------------------

library_quiet <- function(pkgs, attach = TRUE) {
  # pkgs: character vector
  # Loads packages and logs version immediately.
  for (p in pkgs) {
    ok <- require(p, character.only = TRUE, quietly = TRUE, warn.conflicts = FALSE)
    if (!ok) {
      log_error(glue("Failed to load package: {p}"))
      stop("Missing package: ", p)
    }
    if (attach) {
      ver <- tryCatch(as.character(utils::packageVersion(p)), error = function(e) NA_character_)
      log_info(glue("Loaded package: {p} ({ver})"))
    }
  }
  invisible(TRUE)
}

# ---- Diagnostic CSV ---------------------------------------------------------

write_diagnostic <- function(
    file,
    script_id,
    year,
    n_patches,
    n_matched,
    n_new_lineages,
    time_sec,
    warnings = NA,
    errors = NA
) {
  row <- tibble(
    timestamp       = Sys.time(),
    script_id       = script_id,
    year            = year,
    n_patches       = n_patches,
    n_matched       = n_matched,
    n_new_lineages  = n_new_lineages,
    time_sec        = time_sec,
    warnings        = warnings,
    errors          = errors
  )
  
  tryCatch({
    if (!file.exists(file)) {
      readr::write_csv(row, file)
    } else {
      readr::write_csv(row, file, append = TRUE)
    }
  }, error = function(e) {
    # Don't ever crash the pipeline because diagnostics failed
    invisible(NULL)
  })
}