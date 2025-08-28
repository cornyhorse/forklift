-- Create or alter SALES user (idempotent); clear error if a ROLE named SALES exists
BEGIN
  BEGIN
    -- Try alter first (fast path when user already exists)
    EXECUTE IMMEDIATE 'ALTER USER SALES IDENTIFIED BY "DevPass123"';
    EXECUTE IMMEDIATE 'ALTER USER SALES QUOTA UNLIMITED ON USERS';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -1918 THEN
        -- ORA-01918: user 'SALES' does not exist -> try to create
        BEGIN
          EXECUTE IMMEDIATE 'CREATE USER SALES IDENTIFIED BY "DevPass123" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON USERS';
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE = -1920 THEN
              -- ORA-01920: conflicts with another user or role name
              RAISE_APPLICATION_ERROR(-20001, 'Conflicting ROLE named SALES exists. Drop/rename the role or change the username.');
            ELSE
              RAISE;
            END IF;
        END;
      ELSE
        RAISE;
      END IF;
  END;
END;
/

-- Create or alter ALT user (idempotent); clear error if a ROLE named ALT exists
BEGIN
  BEGIN
    EXECUTE IMMEDIATE 'ALTER USER ALT IDENTIFIED BY "DevPass123"';
    EXECUTE IMMEDIATE 'ALTER USER ALT QUOTA UNLIMITED ON USERS';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE = -1918 THEN
        -- ORA-01918: user 'ALT' does not exist -> try to create
        BEGIN
          EXECUTE IMMEDIATE 'CREATE USER ALT IDENTIFIED BY "DevPass123" DEFAULT TABLESPACE USERS TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON USERS';
        EXCEPTION
          WHEN OTHERS THEN
            IF SQLCODE = -1920 THEN
              RAISE_APPLICATION_ERROR(-20002, 'Conflicting ROLE named ALT exists. Drop/rename the role or change the username.');
            ELSE
              RAISE;
            END IF;
        END;
      ELSE
        RAISE;
      END IF;
  END;
END;
/

BEGIN
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO SALES';
  EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO SALES';
  EXECUTE IMMEDIATE 'GRANT CREATE SEQUENCE TO SALES';
  EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO SALES';
END;
/

BEGIN
  EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO ALT';
  EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO ALT';
  EXECUTE IMMEDIATE 'GRANT CREATE SEQUENCE TO ALT';
  EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO ALT';
END;
/

-- ========== SALES ==========
ALTER SESSION SET CURRENT_SCHEMA = SALES;

-- Tables
BEGIN
  BEGIN
    EXECUTE IMMEDIATE q'[
      CREATE TABLE good_customers (
        id            NUMBER(10) PRIMARY KEY,
        name          VARCHAR2(255) NOT NULL,
        email         VARCHAR2(320) NOT NULL,
        signup_date   DATE NOT NULL,
        active        NUMBER(1) NOT NULL CHECK (active IN (0,1)),
        amount_usd    NUMBER(18,2) NOT NULL CHECK (amount_usd >= 0),
        country       CHAR(2) NOT NULL,
        status        VARCHAR2(16) NOT NULL CHECK (status IN ('active','inactive','prospect')),
        discount_pct  NUMBER(5,2),
        notes         CLOB
      )
    ]';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN  -- ORA-00955: name is already used by an existing object
        RAISE;
      END IF;
  END;
END;
/

BEGIN
  BEGIN
    EXECUTE IMMEDIATE q'[
      CREATE TABLE purchases (
        purchase_id   NUMBER(10) PRIMARY KEY,
        customer_id   NUMBER(10) NOT NULL REFERENCES good_customers(id),
        purchase_date DATE NOT NULL,
        item          VARCHAR2(255) NOT NULL,
        amount_usd    NUMBER(18,2) NOT NULL CHECK (amount_usd >= 0)
      )
    ]';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END;
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
INSERT INTO good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes)
SELECT 1,'Amy Adams','amy.adams@example.com',DATE '2024-01-05',1,19.99,'US','active',0,'First purchase'
FROM dual
WHERE NOT EXISTS (SELECT 1 FROM good_customers WHERE id=1);

INSERT INTO purchases (purchase_id,customer_id,purchase_date,item,amount_usd)
SELECT 1,1,DATE '2024-01-05','Starter Kit',19.99
FROM dual
WHERE NOT EXISTS (SELECT 1 FROM purchases WHERE purchase_id=1);

COMMIT;

-- ========== ALT ==========
ALTER SESSION SET CURRENT_SCHEMA = ALT;

BEGIN
  BEGIN
    EXECUTE IMMEDIATE q'[
      CREATE TABLE good_customers (
        id            NUMBER(10) PRIMARY KEY,
        name          VARCHAR2(255) NOT NULL,
        email         VARCHAR2(320) NOT NULL,
        signup_date   DATE NOT NULL,
        active        NUMBER(1) NOT NULL CHECK (active IN (0,1)),
        amount_usd    NUMBER(18,2) NOT NULL CHECK (amount_usd >= 0),
        country       CHAR(2) NOT NULL,
        status        VARCHAR2(16) NOT NULL CHECK (status IN ('active','inactive','prospect')),
        discount_pct  NUMBER(5,2),
        notes         CLOB
      )
    ]';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END;
END;
/

BEGIN
  BEGIN
    EXECUTE IMMEDIATE q'[
      CREATE TABLE purchases (
        purchase_id   NUMBER(10) PRIMARY KEY,
        customer_id   NUMBER(10) NOT NULL REFERENCES good_customers(id),
        purchase_date DATE NOT NULL,
        item          VARCHAR2(255) NOT NULL,
        amount_usd    NUMBER(18,2) NOT NULL CHECK (amount_usd >= 0)
      )
    ]';
  EXCEPTION
    WHEN OTHERS THEN
      IF SQLCODE != -955 THEN
        RAISE;
      END IF;
  END;
END;
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

INSERT INTO good_customers (id,name,email,signup_date,active,amount_usd,country,status,discount_pct,notes)
SELECT 101,'Alt Alice','alt.alice@example.com',DATE '2024-04-01',1,12.34,'US','prospect',0,'ALT schema'
FROM dual
WHERE NOT EXISTS (SELECT 1 FROM good_customers WHERE id=101);

INSERT INTO purchases (purchase_id,customer_id,purchase_date,item,amount_usd)
SELECT 1001,101,DATE '2024-04-01','Alt Trial',12.34
FROM dual
WHERE NOT EXISTS (SELECT 1 FROM purchases WHERE purchase_id=1001);
COMMIT;