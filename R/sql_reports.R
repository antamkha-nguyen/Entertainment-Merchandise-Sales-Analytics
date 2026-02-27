pkgs       <- c("DBI", "duckdb", "dplyr", "readr")
to_install <- pkgs[!sapply(pkgs, requireNamespace, quietly = TRUE)]
if (length(to_install) > 0) install.packages(to_install)

library(DBI)
library(duckdb)
library(dplyr)
library(readr)

db_path    <- "business.duckdb"
output_dir <- "outputs"
if (!dir.exists(output_dir)) dir.create(output_dir, recursive = TRUE)

con <- dbConnect(duckdb::duckdb(), dbdir = db_path)

query_db <- function(sql) dbGetQuery(con, sql)

round_numeric_cols <- function(df, digits = 2) {
  df %>% mutate(across(where(is.numeric), ~ round(.x, digits)))
}

save_output <- function(df, filename_base) {
  df   <- round_numeric_cols(df)
  path <- file.path(output_dir, paste0(filename_base, ".csv"))
  write_csv(df, path)
  message("Saved: ", path)
  invisible(path)
}

run_step <- function(step_name, expr) {
  cat("\n---", step_name, "---\n")
  tryCatch(expr, error = function(e) {
    cat("FAILED:", step_name, "\n", conditionMessage(e), "\n")
    NULL
  })
}


# Customer purchase volume
# Aggregate total units purchased per customer to support
# account segmentation and value ranking
run_step("CustomerSalesSummary", {
  df <- query_db("
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           SUM(si.quantity)                   AS total_purchased
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
    GROUP BY full_name
  ")
  save_output(df, "CustomerSalesSummary")
})


# Product shipment volume
# Measure total units shipped per product to identify
# which items are driving outbound fulfillment activity
run_step("ProductShipmentTotals", {
  df <- query_db("
    SELECT p.product_name,
           SUM(sh.quantity_sold) AS total_shipped
    FROM Products p
    JOIN Shipments sh USING (product_id)
    GROUP BY p.product_name
  ")
  save_output(df, "ProductShipmentTotals")
})


# Fulfillment accuracy check
# Pull transactions where the invoiced quantity matches what was shipped.
# These are the clean records; anything excluded here warrants investigation
run_step("MatchedSalesShipments", {
  df <- query_db("
    SELECT si.sales_id,
           si.date AS sales_date,
           sh.shipment_id,
           sh.date AS shipment_date
    FROM Sales_Invoices si
    JOIN Shipments sh USING (sales_id)
    WHERE si.quantity = sh.quantity_sold
  ")
  save_output(df, "MatchedSalesShipments")
})


# Revenue exposure for creditworthy customers
# Break down spend by product for customers rated Good,
# useful for credit-risk-adjusted revenue reporting
run_step("GoodCreditPurchases", {
  df <- query_db("
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           p.product_name,
           (si.unit_price * si.quantity)       AS total_cost
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
    JOIN Products p        USING (product_id)
    WHERE c.credit_rating = 'Good'
  ")
  save_output(df, "GoodCreditPurchases")
})


# Recent inbound inventory activity
# List products received after the Q3 cutoff, ordered newest first,
# to track late-year restocking for seasonal planning
run_step("RecentReceipts", {
  df <- query_db("
    SELECT p.product_name,
           r.date AS receipt_date
    FROM Receipts r
    JOIN Products p USING (product_id)
    WHERE CAST(r.date AS DATE) > DATE '2024-09-01'
    ORDER BY CAST(r.date AS DATE) DESC
  ")
  save_output(df, "RecentReceipts")
})


# Shipment log by customer
# Pull every shipment with its customer and quantity,
# ordered alphabetically for easy lookup and service tracking
run_step("CustomerShipments", {
  df <- query_db("
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           sh.date                             AS shipment_date,
           sh.quantity_sold
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
    JOIN Shipments sh      USING (sales_id)
    ORDER BY c.last_name
  ")
  save_output(df, "CustomerShipments")
})


# Sales activity on low-stock products
# Flag products with fewer than 200 units on hand and summarize
# their revenue and volume to assess stockout risk
run_step("LowInventorySales", {
  df <- query_db("
    SELECT p.product_name,
           SUM(si.quantity)                 AS total_quantity_sold,
           SUM(si.unit_price * si.quantity) AS total_revenue
    FROM Products p
    JOIN Sales_Invoices si USING (product_id)
    WHERE p.inventory_amount < 200
    GROUP BY p.product_name
  ")
  save_output(df, "LowInventorySales")
})


# Regional inventory flow for NY customers
# Match what NY customers purchased against total receipts per product.
# Pre-aggregate each side in CTEs before joining to avoid row multiplication
# from combining two independent fact tables on a shared product key
run_step("NYCustomerReceipts", {
  df <- query_db("
    WITH ny_sales AS (
      SELECT si.product_id,
             c.first_name || ' ' || c.last_name AS full_name,
             SUM(si.quantity)                   AS total_quantity_purchased
      FROM Customers c
      JOIN Sales_Invoices si USING (customer_id)
      WHERE c.address LIKE '%NY%'
      GROUP BY si.product_id, full_name
    ),
    receipts_by_product AS (
      SELECT product_id,
             SUM(quantity_received) AS total_quantity_received
      FROM Receipts
      GROUP BY product_id
    )
    SELECT ny.full_name,
           p.product_name,
           COALESCE(rp.total_quantity_received, 0) AS total_quantity_received
    FROM ny_sales ny
    JOIN Products p                  USING (product_id)
    LEFT JOIN receipts_by_product rp USING (product_id)
    ORDER BY ny.full_name, p.product_name
  ")
  save_output(df, "NYCustomerReceipts")
})


# Open orders with no shipment
# Detect invoices with no matching shipment row using a LEFT JOIN null check
# These represent unfulfilled orders that need operational follow-up
run_step("UnshippedSales", {
  df <- query_db("
    SELECT si.sales_id,
           si.date,
           si.quantity
    FROM Sales_Invoices si
    LEFT JOIN Shipments sh USING (sales_id)
    WHERE sh.sales_id IS NULL
  ")
  save_output(df, "UnshippedSales")
})


# Product profitability screening
# Calculate gross profit as (selling_price - cost) * quantity_sold and surface
# products that clear a minimum profit threshold
run_step("ProfitableProducts", {
  df <- query_db("
    WITH profit_by_product AS (
      SELECT p.product_id,
             p.product_name,
             SUM(sh.quantity_sold)                               AS total_quantity_sold,
             SUM((p.selling_price - p.cost) * sh.quantity_sold)  AS total_profit
      FROM Products p
      JOIN Shipments sh USING (product_id)
      GROUP BY p.product_id, p.product_name
    )
    SELECT product_name, total_quantity_sold, total_profit
    FROM profit_by_product
    WHERE total_profit > 100
  ")
  save_output(df, "ProfitableProducts")
})


# Customer product affinity map
# List each distinct product a customer has purchased to support
# cross-sell analysis and portfolio segmentation.
run_step("CustomerPurchasePatterns", {
  df <- query_db("
    SELECT DISTINCT c.first_name || ' ' || c.last_name AS full_name,
                    si.product_id
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
    ORDER BY c.last_name, si.product_id
  ")
  save_output(df, "CustomerPurchasePatterns")
})


# Inbound vs outbound inventory balance
# Compare total received against total shipped per product to spot
# where supply and demand are misaligned across the catalog
run_step("ReceiptsVsShipments", {
  df <- query_db("
    WITH r AS (
      SELECT product_id, SUM(quantity_received) AS total_received
      FROM Receipts
      GROUP BY product_id
    ),
    s AS (
      SELECT product_id, SUM(quantity_sold) AS total_sold
      FROM Shipments
      GROUP BY product_id
    )
    SELECT p.product_name,
           COALESCE(r.total_received, 0) AS total_received,
           COALESCE(s.total_sold, 0)     AS total_sold
    FROM Products p
    LEFT JOIN r USING (product_id)
    LEFT JOIN s USING (product_id)
    ORDER BY p.product_name
  ")
  save_output(df, "ReceiptsVsShipments")
})


# Top 5 customers by total spend
# Rank customers by invoice value to identify the accounts
# that matter most for retention and relationship management
run_step("TopCustomerSpending", {
  df <- query_db("
    WITH customer_spend AS (
      SELECT customer_id,
             SUM(unit_price * quantity) AS total_spent
      FROM Sales_Invoices
      GROUP BY customer_id
    )
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           cs.total_spent
    FROM customer_spend cs
    JOIN Customers c USING (customer_id)
    ORDER BY cs.total_spent DESC
    LIMIT 5
  ")
  save_output(df, "TopCustomerSpending")
})


# Inventory timing validation
# Confirm each product was on hand before it was sold by comparing
# the earliest receipt date per product against the invoice date.
# Only sales with a prior receipt are returned; gaps surface as missing rows
run_step("SalesWithReceipts", {
  df <- query_db("
    WITH first_receipt AS (
      SELECT product_id,
             MIN(CAST(date AS DATE)) AS first_receipt_date
      FROM Receipts
      GROUP BY product_id
    )
    SELECT p.product_name,
           CAST(si.date AS DATE) AS sales_date,
           fr.first_receipt_date AS receipt_date
    FROM Sales_Invoices si
    JOIN first_receipt fr USING (product_id)
    JOIN Products p       USING (product_id)
    WHERE fr.first_receipt_date < CAST(si.date AS DATE)
  ")
  save_output(df, "SalesWithReceipts")
})


# Fulfillment volume by credit rating
# Aggregate shipped quantities grouped by customer credit rating
# to understand how fulfillment activity distributes across risk tiers
run_step("ShipmentsByCredit", {
  df <- query_db("
    SELECT c.credit_rating,
           SUM(sh.quantity_sold) AS total_quantity_sold
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
    JOIN Shipments sh      USING (sales_id)
    GROUP BY c.credit_rating
    ORDER BY c.credit_rating
  ")
  save_output(df, "ShipmentsByCredit")
})


# Products with no sales history
# Identify catalog items never appearing in an invoice
# using a LEFT JOIN null check, flagging dead stock for review
run_step("UnsoldProducts", {
  df <- query_db("
    SELECT p.product_name, p.inventory_amount
    FROM Products p
    LEFT JOIN Sales_Invoices si USING (product_id)
    WHERE si.product_id IS NULL
  ")
  save_output(df, "UnsoldProducts")
})


# Shipping delay log
# Calculate the gap in days between invoice date and shipment date
# for every late order to support logistics performance review
run_step("ShipmentDelays", {
  df <- query_db("
    SELECT c.first_name || ' ' || c.last_name           AS full_name,
           CAST(si.date AS DATE)                         AS sales_date,
           CAST(sh.date AS DATE)                         AS shipment_date,
           CAST(sh.date AS DATE) - CAST(si.date AS DATE) AS delay_days
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
    JOIN Shipments sh      USING (sales_id)
    WHERE CAST(sh.date AS DATE) > CAST(si.date AS DATE)
  ")
  save_output(df, "ShipmentDelays")
})


# High-value receipt events
# Join receipt dates to same-day sales totals per product and surface
# receipt events coinciding with daily sales above $500,
# highlighting the inbound activity most critical to revenue continuity
run_step("HighValueReceipts", {
  df <- query_db("
    WITH sales_value_by_day AS (
      SELECT product_id,
             CAST(date AS DATE)         AS sales_date,
             SUM(unit_price * quantity) AS total_sales_value
      FROM Sales_Invoices
      GROUP BY product_id, CAST(date AS DATE)
    )
    SELECT p.product_name,
           CAST(r.date AS DATE) AS receipt_date,
           sv.total_sales_value
    FROM Receipts r
    JOIN sales_value_by_day sv
      ON  sv.product_id = r.product_id
      AND sv.sales_date = CAST(r.date AS DATE)
    JOIN Products p ON p.product_id = r.product_id
    WHERE sv.total_sales_value > 500
  ")
  save_output(df, "HighValueReceipts")
})


# Stockout risk assessment
# Estimate remaining inventory by subtracting total units shipped
# from the recorded inventory amount, flagging products below 50 units
run_step("LowStockSales", {
  df <- query_db("
    WITH sold AS (
      SELECT product_id, SUM(quantity_sold) AS total_sold
      FROM Shipments
      GROUP BY product_id
    )
    SELECT p.product_name,
           sold.total_sold,
           p.inventory_amount - sold.total_sold AS remaining_inventory
    FROM Products p
    JOIN sold USING (product_id)
    WHERE (p.inventory_amount - sold.total_sold) < 50
  ")
  save_output(df, "LowStockSales")
})


# End-to-end revenue attribution
# Join customers, products, invoices, and shipments into one report showing
# who bought what, how much shipped, and total revenue per combination,
# ranked highest to lowest
run_step("FullSalesSummary", {
  df <- query_db("
    WITH revenue_by_customer_product AS (
      SELECT si.customer_id,
             si.product_id,
             SUM(sh.quantity_sold)            AS total_quantity_sold,
             SUM(si.unit_price * si.quantity) AS total_revenue
      FROM Sales_Invoices si
      JOIN Shipments sh USING (sales_id)
      GROUP BY si.customer_id, si.product_id
    )
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           p.product_name,
           r.total_quantity_sold,
           r.total_revenue
    FROM revenue_by_customer_product r
    JOIN Customers c USING (customer_id)
    JOIN Products p  USING (product_id)
    ORDER BY r.total_revenue DESC
  ")
  save_output(df, "FullSalesSummary")
})


dbDisconnect(con, shutdown = TRUE)
message("\nSQL reports complete. Outputs saved to: ", output_dir)