if (!requireNamespace("DBI",    quietly = TRUE)) install.packages("DBI")
if (!requireNamespace("duckdb", quietly = TRUE)) install.packages("duckdb")

library(DBI)
library(duckdb)

schema_path <- "sql/schema.sql"
db_path     <- "business.duckdb"

if (!file.exists(schema_path)) stop("sql/schema.sql not found. Check your working directory.")

if (file.exists(db_path)) {
  stop(
    db_path, " already exists. ",
    "Delete it manually or run run_all.R which handles cleanup automatically."
  )
}

con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

schema_text <- paste(readLines(schema_path, warn = FALSE), collapse = "\n")
stmts       <- trimws(unlist(strsplit(schema_text, ";", fixed = TRUE)))
stmts       <- stmts[nzchar(stmts) & !grepl("^--", stmts)]

for (s in stmts) dbExecute(con, paste0(s, ";"))

dbDisconnect(con, shutdown = TRUE)
cat("Database created:", db_path, "\n")
