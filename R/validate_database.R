# Validate the loaded database before running analytics.
# Checks row counts, referential integrity, and expected data characteristics.
# Saves results to outputs/validation_log.txt for a persistent record.

if (!requireNamespace("DBI",    quietly = TRUE)) install.packages("DBI")
if (!requireNamespace("duckdb", quietly = TRUE)) install.packages("duckdb")

library(DBI)
library(duckdb)

db_path    <- "business.duckdb"
output_dir <- "outputs"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

log_path <- file.path(output_dir, "validation_log.txt")
con      <- dbConnect(duckdb::duckdb(), dbdir = db_path)

run_check <- function(title, sql) {
  cat("\n---", title, "---\n")
  res <- dbGetQuery(con, sql)
  print(res)
  invisible(res)
}

sink(log_path, split = TRUE)

cat("Validation run:", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\n")

run_check("Tables in database", "SHOW TABLES;")

run_check("Row counts", "
  SELECT 'Customers'      AS tbl, COUNT(*) AS row_count FROM Customers      UNION ALL
  SELECT 'Products'       AS tbl, COUNT(*) AS row_count FROM Products       UNION ALL
  SELECT 'Sales_Invoices' AS tbl, COUNT(*) AS row_count FROM Sales_Invoices UNION ALL
  SELECT 'Shipments'      AS tbl, COUNT(*) AS row_count FROM Shipments      UNION ALL
  SELECT 'Receipts'       AS tbl, COUNT(*) AS row_count FROM Receipts;
")

run_check("Unshipped sales (expect ~10% of invoices)", "
  SELECT COUNT(*) AS unshipped_sales
  FROM Sales_Invoices si
  LEFT JOIN Shipments sh USING (sales_id)
  WHERE sh.sales_id IS NULL;
")

run_check("Delayed shipments (expect > 0)", "
  SELECT COUNT(*) AS delayed_shipments
  FROM Shipments sh
  JOIN Sales_Invoices si USING (sales_id)
  WHERE CAST(sh.date AS DATE) > CAST(si.date AS DATE);
")

run_check("Quantity match vs mismatch (expect ~4% mismatch)", "
  SELECT
    SUM(CASE WHEN sh.quantity_sold =  si.quantity THEN 1 ELSE 0 END) AS matched_rows,
    SUM(CASE WHEN sh.quantity_sold != si.quantity THEN 1 ELSE 0 END) AS mismatched_rows
  FROM Shipments sh
  JOIN Sales_Invoices si USING (sales_id);
")

run_check("Orphan check (all values should be 0)", "
  SELECT
    (SELECT COUNT(*) FROM Sales_Invoices si LEFT JOIN Customers c      USING (customer_id) WHERE c.customer_id IS NULL)  AS sales_missing_customer,
    (SELECT COUNT(*) FROM Sales_Invoices si LEFT JOIN Products p        USING (product_id)  WHERE p.product_id  IS NULL)  AS sales_missing_product,
    (SELECT COUNT(*) FROM Shipments sh      LEFT JOIN Sales_Invoices si USING (sales_id)    WHERE si.sales_id   IS NULL) AS shipments_missing_sale,
    (SELECT COUNT(*) FROM Receipts r        LEFT JOIN Products p        USING (product_id)  WHERE p.product_id  IS NULL) AS receipts_missing_product;
")

sink()

dbDisconnect(con, shutdown = TRUE)
cat("\nValidation complete. Log saved to:", log_path, "\n")
