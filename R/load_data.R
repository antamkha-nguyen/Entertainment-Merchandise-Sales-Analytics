# Load CSV files from data/ into the DuckDB database
# Run after setup_database.R

if (!requireNamespace("DBI",    quietly = TRUE)) install.packages("DBI")
if (!requireNamespace("duckdb", quietly = TRUE)) install.packages("duckdb")

library(DBI)
library(duckdb)

db_path <- "business.duckdb"
csv_dir <- "data"

con <- dbConnect(duckdb::duckdb(), dbdir = db_path)
dbExecute(con, "PRAGMA enable_progress_bar=false;")

dbExecute(con, sprintf("
  INSERT INTO Customers
  SELECT * FROM read_csv_auto('%s/Customers.csv', header = true);
", csv_dir))

dbExecute(con, sprintf("
  INSERT INTO Products
  SELECT * FROM read_csv_auto('%s/Products.csv', header = true);
", csv_dir))

dbExecute(con, sprintf("
  INSERT INTO Sales_Invoices (sales_id, customer_id, product_id, unit_price, quantity, \"date\")
  SELECT
    CAST(sales_id    AS BIGINT),
    CAST(customer_id AS BIGINT),
    CAST(product_id  AS BIGINT),
    CAST(unit_price  AS DOUBLE),
    CAST(quantity    AS BIGINT),
    CAST(\"date\"    AS DATE)
  FROM read_csv_auto('%s/Sales_Invoices.csv', header = true);
", csv_dir))

dbExecute(con, sprintf("
  INSERT INTO Shipments (shipment_id, sales_id, product_id, quantity_sold, \"date\")
  SELECT
    CAST(shipment_id   AS BIGINT),
    CAST(sales_id      AS BIGINT),
    CAST(product_id    AS BIGINT),
    CAST(quantity_sold AS BIGINT),
    CAST(\"date\"      AS DATE)
  FROM read_csv_auto('%s/Shipments.csv', header = true);
", csv_dir))

dbExecute(con, sprintf("
  INSERT INTO Receipts (receipt_id, product_id, quantity_received, \"date\")
  SELECT
    CAST(receipt_id        AS BIGINT),
    CAST(product_id        AS BIGINT),
    CAST(quantity_received AS BIGINT),
    CAST(\"date\"          AS DATE)
  FROM read_csv_auto('%s/Receipts.csv', header = true);
", csv_dir))

tables <- c("Customers", "Products", "Sales_Invoices", "Shipments", "Receipts")
counts <- data.frame(
  table     = tables,
  row_count = sapply(tables, function(t) {
    dbGetQuery(con, paste0("SELECT COUNT(*) AS n FROM ", t))$n[1]
  })
)

cat("\nRow counts after load:\n")
print(counts, row.names = FALSE)

dbDisconnect(con, shutdown = TRUE)
cat("\nLoad complete:", db_path, "\n")
