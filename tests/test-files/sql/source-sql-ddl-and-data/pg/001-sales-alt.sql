-- Connect the container to DB 'testdb' (compose already creates it).
-- Two schemas
CREATE SCHEMA IF NOT EXISTS sales;
CREATE SCHEMA IF NOT EXISTS alt;

-- ========== sales ==========
CREATE TABLE IF NOT EXISTS sales.good_customers (
  id            INTEGER PRIMARY KEY CHECK (id >= 1),
  name          TEXT    NOT NULL CHECK (length(name) >= 1),
  email         TEXT    NOT NULL,
  signup_date   DATE    NOT NULL,
  active        BOOLEAN NOT NULL,
  amount_usd    NUMERIC(18,2) NOT NULL CHECK (amount_usd >= 0),
  country       CHAR(2) NOT NULL,
  status        TEXT    NOT NULL CHECK (status IN ('active','inactive','prospect')),
  discount_pct  NUMERIC(5,2),
  notes         TEXT
);

CREATE TABLE IF NOT EXISTS sales.purchases (
  purchase_id   INTEGER PRIMARY KEY,
  customer_id   INTEGER NOT NULL REFERENCES sales.good_customers(id),
  purchase_date DATE    NOT NULL,
  item          TEXT    NOT NULL,
  amount_usd    NUMERIC(18,2) NOT NULL CHECK (amount_usd >= 0)
);

CREATE OR REPLACE VIEW sales.v_good_customers AS
SELECT
  id, name, email, country,
  CASE WHEN active THEN 'Yes' ELSE 'No' END AS active_status,
  status, amount_usd, COALESCE(discount_pct,0) AS discount_pct,
  ROUND(amount_usd * (1 - COALESCE(discount_pct,0)/100.0), 2) AS net_amount,
  signup_date, notes
FROM sales.good_customers;

-- sales data (20 rows)
INSERT INTO sales.good_customers
(id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes) VALUES
(1,'Amy Adams','amy.adams@example.com','2024-01-05',TRUE,19.99,'US','active',0,'First purchase'),
(2,'Ben Baker','ben.baker@example.com','2024-01-06',TRUE,49.00,'US','active',10,'Loyalty promo'),
(3,'Chad Chen','chad.chen@example.com','2024-01-07',FALSE,0.00,'CA','inactive',0,'Churned'),
(4,'Dara Diaz','dara.diaz@example.com','2024-01-08',TRUE,120.50,'MX','active',5,'High value'),
(5,'Eli Evans','eli.evans@example.com','2024-01-09',TRUE,9.99,'US','prospect',0,'Trial'),
(6,'Fay Fang','fay.fang@example.co.uk','2024-02-01',TRUE,39.95,'GB','active',0,'—'),
(7,'Gus Green','gus.green@example.com','2024-02-02',TRUE,18.75,'US','active',0,'Cross-sell'),
(8,'Hui Huang','hui.huang@example.cn','2024-02-03',TRUE,77.10,'CN','active',15,'Holiday sale'),
(9,'Ian Irwin','ian.irwin@example.ie','2024-02-04',FALSE,0.00,'IE','inactive',0,'Refunded'),
(10,'Jia Jin','jia.jin@example.sg','2024-02-05',TRUE,55.55,'SG','active',0,'APAC'),
(11,'Kai Kim','kai.kim@example.kr','2024-03-01',TRUE,12.00,'KR','active',0,'Addon'),
(12,'Liz Lee','liz.lee@example.com','2024-03-02',TRUE,25.00,'US','active',0,'Email opt-in'),
(13,'Max Ma','max.ma@example.com','2024-03-03',TRUE,30.00,'US','active',20,'Coupon'),
(14,'Nia Nash','nia.nash@example.com','2024-03-04',TRUE,60.00,'US','active',0,'Upsell'),
(15,'Ola O''Neil','ola.oneil@example.ie','2024-03-05',TRUE,15.49,'IE','active',0,'Name with apostrophe'),
(16,'Pia Park','pia.park@example.com','2024-03-06',TRUE,99.99,'US','active',0,'Big basket'),
(17,'Qin Qi','qin.qi@example.cn','2024-03-07',TRUE,8.00,'CN','active',0,'—'),
(18,'Raj Rao','raj.rao@example.in','2024-03-08',TRUE,22.25,'IN','active',0,'India launch'),
(19,'Sue Sun','sue.sun@example.com','2024-03-09',TRUE,11.11,'US','active',0,'Round trip'),
(20,'Ted Tran','ted.tran@example.com','2024-03-10',TRUE,200.00,'US','active',0,'Whale')
ON CONFLICT (id) DO NOTHING;

INSERT INTO sales.purchases (purchase_id,customer_id,purchase_date,item,amount_usd) VALUES
(1,1,'2024-01-05','Starter Kit',19.99),
(2,2,'2024-01-10','Subscription',49.00),
(3,2,'2024-02-15','Addon Pack',15.00),
(4,4,'2024-01-08','Premium Bundle',120.50),
(5,5,'2024-01-12','Trial Extension',9.99),
(6,6,'2024-02-01','Special Edition',39.95),
(7,7,'2024-02-02','Cross-sell Pack',18.75),
(8,8,'2024-02-03','Holiday Bundle',77.10),
(9,10,'2024-02-05','APAC Subscription',55.55),
(10,11,'2024-03-01','Mobile Addon',12.00),
(11,12,'2024-03-02','Email Promo',25.00),
(12,13,'2024-03-03','Coupon Deal',24.00),
(13,13,'2024-03-15','Coupon Deal',6.00),
(14,14,'2024-03-04','Upsell Offer',60.00),
(15,15,'2024-03-05','IE Discount Pack',15.49),
(16,16,'2024-03-06','Large Basket',99.99),
(17,18,'2024-03-08','India Launch Pack',22.25),
(18,19,'2024-03-09','Return Trip',11.11),
(19,20,'2024-03-10','Whale Package',200.00),
(20,20,'2024-03-20','Whale Addon',150.00)
ON CONFLICT (purchase_id) DO NOTHING;

-- ========== alt ==========
CREATE TABLE IF NOT EXISTS alt.good_customers (LIKE sales.good_customers INCLUDING ALL);
CREATE TABLE IF NOT EXISTS alt.purchases      (LIKE sales.purchases INCLUDING ALL);

CREATE OR REPLACE VIEW alt.v_good_customers AS
SELECT
  id, name, email, country,
  CASE WHEN active THEN 'Yes' ELSE 'No' END AS active_status,
  status, amount_usd, COALESCE(discount_pct,0) AS discount_pct,
  ROUND(amount_usd * (1 - COALESCE(discount_pct,0)/100.0), 2) AS net_amount,
  signup_date, notes
FROM alt.good_customers;

INSERT INTO alt.good_customers
(id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes) VALUES
(101,'Alt Alice','alt.alice@example.com','2024-04-01',TRUE,12.34,'US','prospect',0,'ALT schema'),
(102,'Alt Bob','alt.bob@example.com','2024-04-02',FALSE,0.00,'US','inactive',0,'ALT schema')
ON CONFLICT (id) DO NOTHING;

INSERT INTO alt.purchases (purchase_id,customer_id,purchase_date,item,amount_usd) VALUES
(1001,101,'2024-04-01','Alt Trial',12.34)
ON CONFLICT (purchase_id) DO NOTHING;