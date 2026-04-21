# Entertainment Merchandise Sales Analytics

An end-to-end business intelligence pipeline built in R and DuckDB, covering sales, inventory, fulfillment, and customer analytics for an entertainment merchandise retailer.

**Stack:** R, SQL (DuckDB)



## What This Covers

**SQL:** INNER and LEFT JOINs, CTEs, GROUP BY and HAVING

**R:** Descriptive statistics, paired t-test, simple linear regression,Pearson correlation, variance analysis



## Key Findings

- **1,266 invoices (10.6%)** had no corresponding shipment, surfacing open order backlog
- **8,491 shipments arrived late**, averaging a **4.2-day delay** from invoice to delivery
- **3.85% of shipments** had a quantity mismatch against the original invoice
- Top customer Emma Garcia spent **$54,189**, over $3,000 more than the next closest account, indicating one dominant buyer in the customer base
- Customers averaged **66 orders** and **8.5 units per order** across the period
- Revenue per transaction was nearly flat across credit tiers: Poor-rated customers averaged **$146**, only slightly above Excellent at **$133**, suggesting credit rating does not strongly predict order size
- **52.8% of customers showed positive revenue growth** over the year with a median growth rate close to flat, indicating a stable rather than expanding base
- Paired t-test on inbound vs outbound quantities: **t = 6.13, p < 0.001**, confirming receipts significantly exceed sales volume and pointing to deliberate buffer stocking across the catalog



## Dataset

Synthetic datasets reflect realistic entertainment merchandise retail patterns:

- 200 customers across 15 US states with credit ratings (Excellent / Good / Fair / Poor)
- 100 products drawn from common merchandise types (Vinyl Record, Poster Print, Collector Figure, etc.)
- Power-law skew: 8% of customers and 6% of products drive the majority of transaction volume
- Q4 and weekend seasonality in date sampling
- 10% of invoices intentionally unshipped to simulate open order backlog
- 4% quantity mismatch between invoices and shipments to simulate fulfillment exceptions
- Receipts backdated to October 2023 to reflect pre-season procurement lead time



## Project Structure

```
.
├── R/
│   ├── setup_database.R       # Create business.duckdb from sql/schema.sql
│   ├── load_data.R            # Load CSVs into the database
│   ├── validate_database.R    # Check row counts, integrity, and expected anomalies
│   ├── sql_reports.R          # Operational SQL reports
│   ├── statistical_analysis.R # R statistical analyses
│   └── run_all.R              # Run the full pipeline in order
├── sql/
│   └── schema.sql             # Table definitions for the five core tables
├── data/                      # CSV datasets
├── outputs/                   # Generated reports (created on run)
├── .gitignore
└── README.md
```



## Usage

Requires R 4.0 or later. All packages install automatically on first run.

```r
# Set working directory to project root, then:
source("R/run_all.R")
```

Or run each script individually in order. Re-running `run_all.R` handles
database cleanup automatically.



## Power BI dashboards

Sales Operational Overview

<img width="800" height="550" alt="Screenshot 1" src="https://github.com/user-attachments/assets/a182327e-1587-4980-b176-1f83586a4f71" />


Customer Stats

<img width="800" height="550" alt="Screenshot 2" src="https://github.com/user-attachments/assets/ec4d019f-1811-46c1-bc0c-e5ebd75834b0" />


Modeling View

<img width="650" height="500" alt="Modeling View" src="https://github.com/user-attachments/assets/3693078b-8910-45d9-9e55-3595dfb86f68" />





