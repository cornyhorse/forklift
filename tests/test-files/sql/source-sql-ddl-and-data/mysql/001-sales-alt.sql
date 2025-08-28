CREATE DATABASE IF NOT EXISTS `sales_db` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;
CREATE DATABASE IF NOT EXISTS `alt_db`   CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci;

-- ========== sales_db ==========
USE `sales_db`;

CREATE TABLE IF NOT EXISTS good_customers (
  id            INT PRIMARY KEY,
  name          VARCHAR(255) NOT NULL,
  email         VARCHAR(320) NOT NULL,
  signup_date   DATE NOT NULL,
  active        TINYINT(1) NOT NULL,               -- boolean-ish
  amount_usd    DECIMAL(18,2) NOT NULL CHECK (amount_usd >= 0),
  country       CHAR(2) NOT NULL,
  status        ENUM('active','inactive','prospect') NOT NULL,
  discount_pct  DECIMAL(5,2),
  notes         TEXT
) ENGINE=InnoDB;

CREATE TABLE IF NOT EXISTS purchases (
  purchase_id   INT PRIMARY KEY,
  customer_id   INT NOT NULL,
  purchase_date DATE NOT NULL,
  item          VARCHAR(255) NOT NULL,
  amount_usd    DECIMAL(18,2) NOT NULL CHECK (amount_usd >= 0),
  CONSTRAINT fk_gc FOREIGN KEY (customer_id) REFERENCES good_customers(id)
) ENGINE=InnoDB;

CREATE OR REPLACE VIEW v_good_customers AS
SELECT
  id, name, email, country,
  IF(active=1,'Yes','No') AS active_status,
  status, amount_usd, COALESCE(discount_pct,0) AS discount_pct,
  ROUND(amount_usd * (1 - COALESCE(discount_pct,0)/100.0), 2) AS net_amount,
  signup_date, notes
FROM good_customers;

-- data
INSERT IGNORE INTO good_customers
(id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes) VALUES
(1,'Amy Adams','amy.adams@example.com','2024-01-05',1,19.99,'US','active',0,'First purchase'),
(2,'Ben Baker','ben.baker@example.com','2024-01-06',1,49.00,'US','active',10,'Loyalty promo'),
(3,'Chad Chen','chad.chen@example.com','2024-01-07',0,0.00,'CA','inactive',0,'Churned'),
(4,'Dara Diaz','dara.diaz@example.com','2024-01-08',1,120.50,'MX','active',5,'High value'),
(5,'Eli Evans','eli.evans@example.com','2024-01-09',1,9.99,'US','prospect',0,'Trial');

INSERT IGNORE INTO purchases (purchase_id,customer_id,purchase_date,item,amount_usd) VALUES
(1,1,'2024-01-05','Starter Kit',19.99),
(2,2,'2024-01-10','Subscription',49.00);

-- ========== alt_db ==========
USE `alt_db`;

CREATE TABLE IF NOT EXISTS good_customers LIKE `sales_db`.good_customers;
CREATE TABLE IF NOT EXISTS purchases      LIKE `sales_db`.purchases;

CREATE OR REPLACE VIEW v_good_customers AS
SELECT
  id, name, email, country,
  IF(active=1,'Yes','No') AS active_status,
  status, amount_usd, COALESCE(discount_pct,0) AS discount_pct,
  ROUND(amount_usd * (1 - COALESCE(discount_pct,0)/100.0), 2) AS net_amount,
  signup_date, notes
FROM good_customers;

INSERT IGNORE INTO good_customers
(id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes) VALUES
(101,'Alt Alice','alt.alice@example.com','2024-04-01',1,12.34,'US','prospect',0,'ALT schema');

INSERT IGNORE INTO purchases (purchase_id,customer_id,purchase_date,item,amount_usd) VALUES
(1001,101,'2024-04-01','Alt Trial',12.34);