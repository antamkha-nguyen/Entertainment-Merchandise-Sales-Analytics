# Run the full pipeline from database setup through analytics
# Set your working directory to the project root before running

if (file.exists("business.duckdb")) {
  file.remove("business.duckdb")
  cat("Removed existing business.duckdb\n")
}

source("R/setup_database.R")
source("R/load_data.R")
source("R/validate_database.R")
source("R/sql_reports.R")
source("R/statistical_analysis.R")
