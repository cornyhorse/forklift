-- USE master;
-- IF DB_ID('testdb') IS NULL CREATE DATABASE testdb;
USE testdb;

IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'sales') EXEC('CREATE SCHEMA sales');
IF NOT EXISTS (SELECT * FROM sys.schemas WHERE name = 'alt')   EXEC('CREATE SCHEMA alt');

-- ========== sales ==========
IF OBJECT_ID('sales.good_customers','U') IS NULL
CREATE TABLE sales.good_customers (
  id            INT NOT NULL CONSTRAINT PK_sales_good_customers PRIMARY KEY,
  name          NVARCHAR(255) NOT NULL,
  email         NVARCHAR(320) NOT NULL,
  signup_date   DATE NOT NULL,
  active        BIT NOT NULL,
  amount_usd    DECIMAL(18,2) NOT NULL CHECK (amount_usd >= 0),
  country       CHAR(2) NOT NULL,
  status        NVARCHAR(16) NOT NULL CHECK (status IN (N'active',N'inactive',N'prospect')),
  discount_pct  DECIMAL(5,2) NULL,
  notes         NVARCHAR(MAX) NULL
);

IF OBJECT_ID('sales.purchases','U') IS NULL
CREATE TABLE sales.purchases (
  purchase_id   INT NOT NULL CONSTRAINT PK_sales_purchases PRIMARY KEY,
  customer_id   INT NOT NULL CONSTRAINT FK_sales_purch_gc FOREIGN KEY REFERENCES sales.good_customers(id),
  purchase_date DATE NOT NULL,
  item          NVARCHAR(255) NOT NULL,
  amount_usd    DECIMAL(18,2) NOT NULL CHECK (amount_usd >= 0)
);

IF OBJECT_ID('sales.v_good_customers','V') IS NOT NULL
    DROP VIEW sales.v_good_customers;
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE VIEW sales.v_good_customers AS
SELECT
  id, name, email, country,
  CASE WHEN active=1 THEN 'Yes' ELSE 'No' END AS active_status,
  status, amount_usd, ISNULL(discount_pct,0) AS discount_pct,
  ROUND(amount_usd * (1 - ISNULL(discount_pct,0)/100.0), 2) AS net_amount,
  signup_date, notes
FROM sales.good_customers;
GO

INSERT INTO sales.good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes)
SELECT 1,'Amy Adams','amy.adams@example.com','2024-01-05',1,19.99,'US','active',0,'First purchase'
WHERE NOT EXISTS (SELECT 1 FROM sales.good_customers WHERE id=1);

INSERT INTO sales.good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes)
SELECT 2,'Ben Baker','ben.baker@example.com','2024-01-06',1,49.00,'US','active',10,'Loyalty promo'
WHERE NOT EXISTS (SELECT 1 FROM sales.good_customers WHERE id=2);

INSERT INTO sales.purchases (purchase_id,customer_id,purchase_date,item,amount_usd)
SELECT 1,1,'2024-01-05','Starter Kit',19.99
WHERE NOT EXISTS (SELECT 1 FROM sales.purchases WHERE purchase_id=1);

-- ========== alt ==========
IF OBJECT_ID('alt.good_customers','U') IS NULL
SELECT TOP 0 * INTO alt.good_customers FROM sales.good_customers;

IF OBJECT_ID('alt.purchases','U') IS NULL
SELECT TOP 0 * INTO alt.purchases FROM sales.purchases;

IF OBJECT_ID('alt.v_good_customers','V') IS NOT NULL
    DROP VIEW alt.v_good_customers;
GO
SET ANSI_NULLS ON;
GO
SET QUOTED_IDENTIFIER ON;
GO
CREATE VIEW alt.v_good_customers AS
SELECT
  id, name, email, country,
  CASE WHEN active=1 THEN 'Yes' ELSE 'No' END AS active_status,
  status, amount_usd, ISNULL(discount_pct,0) AS discount_pct,
  ROUND(amount_usd * (1 - ISNULL(discount_pct,0)/100.0), 2) AS net_amount,
  signup_date, notes
FROM alt.good_customers;
GO

INSERT INTO alt.good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes)
SELECT 101,'Alt Alice','alt.alice@example.com','2024-04-01',1,12.34,'US','prospect',0,'ALT schema'
WHERE NOT EXISTS (SELECT 1 FROM alt.good_customers WHERE id=101);

INSERT INTO alt.purchases (purchase_id,customer_id,purchase_date,item,amount_usd)
SELECT 1001,101,'2024-04-01','Alt Trial',12.34
WHERE NOT EXISTS (SELECT 1 FROM alt.purchases WHERE purchase_id=1001);