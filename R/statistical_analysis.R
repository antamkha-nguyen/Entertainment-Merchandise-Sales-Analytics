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

# Accumulate key metrics from each analysis into a summary table
summary_rows <- list()

add_summary <- function(analysis, metric, value) {
  summary_rows[[length(summary_rows) + 1]] <<- data.frame(
    analysis = analysis,
    metric   = metric,
    value    = round(as.numeric(value), 4),
    stringsAsFactors = FALSE
  )
}


# Customer order behavior profile
# Compute purchase frequency, average order size, and variability per customer
run_step("CustomerPurchaseStats", {
  raw <- query_db("
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           si.quantity
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
  ")

  df <- raw %>%
    group_by(full_name) %>%
    summarise(
      total_purchases  = n(),
      average_quantity = mean(quantity, na.rm = TRUE),
      std_dev_quantity = sd(quantity,   na.rm = TRUE),
      .groups = "drop"
    )

  save_output(df, "CustomerPurchaseStats")

  add_summary("CustomerPurchaseStats", "mean_avg_quantity",    mean(df$average_quantity, na.rm = TRUE))
  add_summary("CustomerPurchaseStats", "mean_purchases_per_customer", mean(df$total_purchases, na.rm = TRUE))
})


# Product margin analysis
# Compute total revenue, total cost, and gross margin percentage per product
run_step("ProductProfitMargins", {
  raw <- query_db("
    SELECT p.product_name,
           si.unit_price,
           si.quantity,
           p.cost
    FROM Products p
    JOIN Sales_Invoices si USING (product_id)
  ")

  df <- raw %>%
    mutate(
      revenue    = unit_price * quantity,
      cost_total = cost * quantity
    ) %>%
    group_by(product_name) %>%
    summarise(
      total_revenue     = sum(revenue,    na.rm = TRUE),
      total_cost        = sum(cost_total, na.rm = TRUE),
      profit_margin_pct = (total_revenue - total_cost) / total_revenue * 100,
      .groups = "drop"
    )

  save_output(df, "ProductProfitMargins")

  add_summary("ProductProfitMargins", "median_margin_pct", median(df$profit_margin_pct, na.rm = TRUE))
  add_summary("ProductProfitMargins", "mean_margin_pct",   mean(df$profit_margin_pct,   na.rm = TRUE))
})


# Shipping delay KPI summary
# Count late shipments and compute the average delay across the full period
run_step("ShipmentDelayStats", {
  raw <- query_db("
    SELECT CAST(si.date AS DATE) AS sales_date,
           CAST(sh.date AS DATE) AS shipment_date
    FROM Sales_Invoices si
    JOIN Shipments sh USING (sales_id)
    WHERE CAST(sh.date AS DATE) > CAST(si.date AS DATE)
  ")

  df <- raw %>%
    mutate(delay_days = as.numeric(as.Date(shipment_date) - as.Date(sales_date))) %>%
    summarise(
      total_delays       = n(),
      average_delay_days = mean(delay_days, na.rm = TRUE)
    )

  save_output(df, "ShipmentDelayStats")

  add_summary("ShipmentDelayStats", "total_delayed_shipments", df$total_delays)
  add_summary("ShipmentDelayStats", "average_delay_days",      df$average_delay_days)
})


# Inventory flow balance test
# Aggregate total received and total sold per product, then run a paired
# t-test to determine whether the difference is statistically significant
# A significant result points to a systematic gap between procurement and sales volume
run_step("ReceiptSalesDiff", {
  raw <- query_db("
    WITH r AS (
      SELECT product_id, SUM(quantity_received) AS total_received
      FROM Receipts
      GROUP BY product_id
    ),
    s AS (
      SELECT product_id, SUM(quantity) AS total_sold
      FROM Sales_Invoices
      GROUP BY product_id
    )
    SELECT p.product_name,
           COALESCE(r.total_received, 0) AS total_received,
           COALESCE(s.total_sold,     0) AS total_sold
    FROM Products p
    LEFT JOIN r USING (product_id)
    LEFT JOIN s USING (product_id)
  ")

  save_output(raw, "ReceiptSalesDiff_Summary")

  tt <- t.test(raw$total_received, raw$total_sold, paired = TRUE)

  sink(file.path(output_dir, "ReceiptSalesDiff_ttest.txt"))
  print(tt)
  sink()
  message("Saved: ", file.path(output_dir, "ReceiptSalesDiff_ttest.txt"))

  add_summary("ReceiptSalesDiff", "t_statistic", tt$statistic)
  add_summary("ReceiptSalesDiff", "p_value",     tt$p.value)
  add_summary("ReceiptSalesDiff", "mean_diff",   tt$estimate)
})


# Customer price-volume sensitivity
# For customers with at least two transactions, regress total spending on quantity
# The slope captures how much additional spend each unit of volume generates
run_step("CustomerSpendingReg", {
  raw <- query_db("
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           si.quantity,
           si.unit_price
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
  ")

  df <- raw %>%
    mutate(spending = unit_price * quantity) %>%
    group_by(full_name) %>%
    filter(n() >= 2) %>%
    summarise(
      intercept = coef(lm(spending ~ quantity))[1],
      slope     = coef(lm(spending ~ quantity))[2],
      .groups = "drop"
    )

  save_output(df, "CustomerSpendingReg")

  add_summary("CustomerSpendingReg", "median_slope",     median(df$slope,     na.rm = TRUE))
  add_summary("CustomerSpendingReg", "median_intercept", median(df$intercept, na.rm = TRUE))
})


# Inventory turnover by product
# Compute turnover rate per shipment as quantity_sold divided by inventory_amount,
# then summarize average rate and variance to identify fast and slow movers
run_step("InventoryTurnoverStats", {
  raw <- query_db("
    SELECT p.product_name,
           p.inventory_amount,
           sh.quantity_sold
    FROM Products p
    JOIN Shipments sh USING (product_id)
  ")

  df <- raw %>%
    mutate(turnover_rate = quantity_sold / inventory_amount) %>%
    group_by(product_name) %>%
    summarise(
      avg_turnover_rate = mean(turnover_rate, na.rm = TRUE),
      turnover_variance = var(turnover_rate,  na.rm = TRUE),
      .groups = "drop"
    )

  save_output(df, "InventoryTurnoverStats")

  add_summary("InventoryTurnoverStats", "mean_turnover_rate",     mean(df$avg_turnover_rate, na.rm = TRUE))
  add_summary("InventoryTurnoverStats", "mean_turnover_variance", mean(df$turnover_variance, na.rm = TRUE))
})


# Revenue distribution by credit rating
# Summarize mean, median, and interquartile range of per-transaction revenue
# for each credit tier to understand how purchase value varies with credit risk
run_step("CreditRatingSummaries", {
  raw <- query_db("
    SELECT c.credit_rating,
           si.unit_price,
           si.quantity
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
  ")

  df <- raw %>%
    mutate(revenue = unit_price * quantity) %>%
    group_by(credit_rating) %>%
    summarise(
      mean_revenue   = mean(revenue,          na.rm = TRUE),
      median_revenue = median(revenue,         na.rm = TRUE),
      q1             = quantile(revenue, 0.25, na.rm = TRUE),
      q3             = quantile(revenue, 0.75, na.rm = TRUE),
      .groups = "drop"
    )

  save_output(df, "CreditRatingSummaries")

  for (i in seq_len(nrow(df))) {
    add_summary("CreditRatingSummaries",
                paste0(df$credit_rating[i], "_mean_revenue"),
                df$mean_revenue[i])
  }
})


# Restock-to-sales regression for high-volume products
# Filter to products with more than 500 units received, then regress
# total sold on total received. A positive slope suggests restocking
# directly drives sales volume for these items
run_step("HighVolumeReg", {
  raw <- query_db("
    WITH r AS (
      SELECT product_id, SUM(quantity_received) AS total_received
      FROM Receipts
      GROUP BY product_id
    ),
    sh AS (
      SELECT product_id, SUM(quantity_sold) AS total_sold
      FROM Shipments
      GROUP BY product_id
    )
    SELECT p.product_name,
           COALESCE(r.total_received, 0) AS total_received,
           COALESCE(sh.total_sold,    0) AS total_sold
    FROM Products p
    LEFT JOIN r  USING (product_id)
    LEFT JOIN sh USING (product_id)
  ")

  high_vol <- raw %>% filter(total_received > 500)
  model    <- lm(total_sold ~ total_received, data = high_vol)
  s        <- summary(model)

  sink(file.path(output_dir, "HighVolumeReg.txt"))
  print(s)
  sink()
  message("Saved: ", file.path(output_dir, "HighVolumeReg.txt"))

  add_summary("HighVolumeReg", "r_squared",          s$r.squared)
  add_summary("HighVolumeReg", "slope_total_received", coef(model)["total_received"])
  add_summary("HighVolumeReg", "p_value_slope",
              summary(model)$coefficients["total_received", "Pr(>|t|)"])
})


# Customer revenue growth rate
# Fit a regression of revenue over time per customer using numeric dates
# The slope estimates how quickly each customer's spend is growing or declining
run_step("CustomerSalesGrowth", {
  raw <- query_db("
    SELECT c.first_name || ' ' || c.last_name AS full_name,
           CAST(si.date AS DATE)               AS sales_date,
           si.unit_price,
           si.quantity
    FROM Customers c
    JOIN Sales_Invoices si USING (customer_id)
  ")

  df <- raw %>%
    mutate(
      revenue      = unit_price * quantity,
      numeric_date = as.numeric(as.Date(sales_date))
    ) %>%
    group_by(full_name) %>%
    filter(n() >= 2) %>%
    summarise(
      growth_rate = coef(lm(revenue ~ numeric_date))[2],
      .groups = "drop"
    )

  save_output(df, "CustomerSalesGrowth")

  add_summary("CustomerSalesGrowth", "median_growth_rate", median(df$growth_rate, na.rm = TRUE))
  add_summary("CustomerSalesGrowth", "pct_positive_growth",
              mean(df$growth_rate > 0, na.rm = TRUE) * 100)
})


# Cross-table supply and revenue correlation
# Pull received, sold, and revenue totals per product from three separate
# aggregation CTEs, then compute the Pearson correlation between inbound
# and outbound quantities alongside summary revenue statistics
run_step("QuantityStats", {
  raw <- query_db("
    WITH r AS (
      SELECT product_id, SUM(quantity_received) AS total_received
      FROM Receipts
      GROUP BY product_id
    ),
    sh AS (
      SELECT product_id, SUM(quantity_sold) AS total_sold
      FROM Shipments
      GROUP BY product_id
    ),
    rev AS (
      SELECT product_id, SUM(unit_price * quantity) AS total_revenue
      FROM Sales_Invoices
      GROUP BY product_id
    )
    SELECT p.product_name,
           COALESCE(r.total_received,  0) AS total_received,
           COALESCE(sh.total_sold,     0) AS total_sold,
           COALESCE(rev.total_revenue, 0) AS total_revenue
    FROM Products p
    LEFT JOIN r   USING (product_id)
    LEFT JOIN sh  USING (product_id)
    LEFT JOIN rev USING (product_id)
  ")

  correlation <- cor(raw$total_received, raw$total_sold, use = "complete.obs")

  df <- raw %>%
    summarise(
      correlation_received_sold = correlation,
      mean_received             = mean(total_received,  na.rm = TRUE),
      mean_sold                 = mean(total_sold,      na.rm = TRUE),
      mean_revenue              = mean(total_revenue,   na.rm = TRUE),
      median_revenue            = median(total_revenue, na.rm = TRUE),
      revenue_variance          = var(total_revenue,    na.rm = TRUE)
    )

  save_output(df, "QuantityStats")

  add_summary("QuantityStats", "correlation_received_sold", correlation)
  add_summary("QuantityStats", "mean_revenue",              df$mean_revenue)
  add_summary("QuantityStats", "revenue_variance",          df$revenue_variance)
})


# Consolidate key metrics from all analyses into one readable summary
if (length(summary_rows) > 0) {
  summary_df <- bind_rows(summary_rows)
  save_output(summary_df, "statistical_summary")
  cat("\nConsolidated summary saved to: outputs/statistical_summary.csv\n")
}

dbDisconnect(con, shutdown = TRUE)
message("\nStatistical analysis complete. Outputs saved to: ", output_dir)
