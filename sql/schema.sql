CREATE TABLE Customers (
  customer_id   INTEGER,
  first_name    VARCHAR,
  last_name     VARCHAR,
  address       VARCHAR,
  credit_rating VARCHAR
);

CREATE TABLE Products (
  product_id       INTEGER,
  product_name     VARCHAR,
  inventory_amount INTEGER,
  cost             DOUBLE,
  selling_price    DOUBLE
);

CREATE TABLE Sales_Invoices (
  sales_id    INTEGER,
  unit_price  DOUBLE,
  quantity    INTEGER,
  date        DATE,
  product_id  INTEGER,
  customer_id INTEGER
);

CREATE TABLE Shipments (
  shipment_id   INTEGER,
  sales_id      INTEGER,
  date          DATE,
  quantity_sold INTEGER,
  product_id    INTEGER
);

CREATE TABLE Receipts (
  receipt_id        INTEGER,
  product_id        INTEGER,
  date              DATE,
  quantity_received INTEGER
);
