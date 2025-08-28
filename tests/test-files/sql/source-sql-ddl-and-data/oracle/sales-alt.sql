-- Create two schemas (users)
CREATE USER SALES IDENTIFIED BY "DevPass123" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON USERS;
CREATE USER ALT   IDENTIFIED BY "DevPass123" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON USERS;

GRANT CONNECT, RESOURCE TO SALES;
GRANT CONNECT, RESOURCE TO ALT;

-- ========== SALES ==========
ALTER SESSION SET CURRENT_SCHEMA = SALES;

-- Tables
BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE good_customers (
      id            NUMBER(10) PRIMARY KEY,
      name          VARCHAR2(255) NOT NULL,
      email         VARCHAR2(320) NOT NULL,
      signup_date   DATE NOT NULL,
      active        NUMBER(1) NOT NULL CHECK (active IN (0,1)),
      amount_usd    NUMBER(18,2) NOT NULL CHECK (amount_usd >= 0),
      country       CHAR(2) NOT NULL,
      status        VARCHAR2(16) NOT NULL CHECK (status IN (''active'',''inactive'',''prospect'')),
      discount_pct  NUMBER(5,2),
      notes         CLOB
    )';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
END;
/

BEGIN
  EXECUTE IMMEDIATE '
    CREATE TABLE purchases (
      purchase_id   NUMBER(10) PRIMARY KEY,
      customer_id   NUMBER(10) NOT NULL REFERENCES good_customers(id),
      purchase_date DATE NOT NULL,
      item          VARCHAR2(255) NOT NULL,
      amount_usd    NUMBER(18,2) NOT NULL CHECK (amount_usd >= 0)
    )';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF;
END;
/

-- View
BEGIN
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW v_good_customers AS
    SELECT
      id, name, email, country,
      CASE WHEN active=1 THEN ''Yes'' ELSE ''No'' END AS active_status,
      status, amount_usd, NVL(discount_pct,0) AS discount_pct,
      ROUND(amount_usd * (1 - NVL(discount_pct,0)/100), 2) AS net_amount,
      signup_date, notes
    FROM good_customers';
END;
/

-- Data
INSERT INTO good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes) VALUES
(1,'Amy Adams','amy.adams@example.com',DATE '2024-01-05',1,19.99,'US','active',0,'First purchase');

INSERT INTO purchases (purchase_id,customer_id,purchase_date,item,amount_usd) VALUES
(1,1,DATE '2024-01-05','Starter Kit',19.99);

COMMIT;

-- ========== ALT ==========
ALTER SESSION SET CURRENT_SCHEMA = ALT;

BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE good_customers AS SELECT * FROM SALES.good_customers WHERE 1=0';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;
/

BEGIN
  EXECUTE IMMEDIATE 'CREATE TABLE purchases AS SELECT * FROM SALES.purchases WHERE 1=0';
EXCEPTION WHEN OTHERS THEN IF SQLCODE != -955 THEN RAISE; END IF; END;
/

BEGIN
  EXECUTE IMMEDIATE 'CREATE OR REPLACE VIEW v_good_customers AS
    SELECT
      id, name, email, country,
      CASE WHEN active=1 THEN ''Yes'' ELSE ''No'' END AS active_status,
      status, amount_usd, NVL(discount_pct,0) AS discount_pct,
      ROUND(amount_usd * (1 - NVL(discount_pct,0)/100), 2) AS net_amount,
      signup_date, notes
    FROM good_customers';
END;
/

INSERT INTO good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes) VALUES
(101,'Alt Alice','alt.alice@example.com',DATE '2024-04-01',1,12.34,'US','prospect',0,'ALT schema');

INSERT INTO purchases (purchase_id,customer_id,purchase_date,item,amount_usd) VALUES
(1001,101,DATE '2024-04-01','Alt Trial',12.34);
COMMIT;