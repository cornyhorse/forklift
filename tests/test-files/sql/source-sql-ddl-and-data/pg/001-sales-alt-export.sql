(forklift) matt@Matts-MacBook-Pro ~/P/f/t/t/sql (main) [1]> bash dump-pg.sh
WARN[0000] /Users/matt/PycharmProjects/forklift/tests/test-files/sql/docker-compose.yaml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion
--
-- PostgreSQL database dump
--

\restrict McEJq9AZwhrjaiAO5hEN7HM8YThPMrcgsJWtgHFZ0HJ8s82LSZXnpefdDMTogaG

-- Dumped from database version 16.10 (Debian 16.10-1.pgdg13+1)
-- Dumped by pg_dump version 16.10 (Debian 16.10-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: alt; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA alt;


--
-- Name: sales; Type: SCHEMA; Schema: -; Owner: -
--

CREATE SCHEMA sales;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: good_customers; Type: TABLE; Schema: alt; Owner: -
--

CREATE TABLE alt.good_customers (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    signup_date date NOT NULL,
    active boolean NOT NULL,
    amount_usd numeric(18,2) NOT NULL,
    country character(2) NOT NULL,
    status text NOT NULL,
    discount_pct numeric(5,2),
    notes text,
    CONSTRAINT good_customers_amount_usd_check CHECK ((amount_usd >= (0)::numeric)),
    CONSTRAINT good_customers_id_check CHECK ((id >= 1)),
    CONSTRAINT good_customers_name_check CHECK ((length(name) >= 1)),
    CONSTRAINT good_customers_status_check CHECK ((status = ANY (ARRAY['active'::text, 'inactive'::text, 'prospect'::text])))
);


--
-- Name: purchases; Type: TABLE; Schema: alt; Owner: -
--

CREATE TABLE alt.purchases (
    purchase_id integer NOT NULL,
    customer_id integer NOT NULL,
    purchase_date date NOT NULL,
    item text NOT NULL,
    amount_usd numeric(18,2) NOT NULL,
    CONSTRAINT purchases_amount_usd_check CHECK ((amount_usd >= (0)::numeric))
);


--
-- Name: v_good_customers; Type: VIEW; Schema: alt; Owner: -
--

CREATE VIEW alt.v_good_customers AS
 SELECT id,
    name,
    email,
    country,
        CASE
            WHEN active THEN 'Yes'::text
            ELSE 'No'::text
        END AS active_status,
    status,
    amount_usd,
    COALESCE(discount_pct, (0)::numeric) AS discount_pct,
    round((amount_usd * ((1)::numeric - (COALESCE(discount_pct, (0)::numeric) / 100.0))), 2) AS net_amount,
    signup_date,
    notes
   FROM alt.good_customers;


--
-- Name: good_customers; Type: TABLE; Schema: sales; Owner: -
--

CREATE TABLE sales.good_customers (
    id integer NOT NULL,
    name text NOT NULL,
    email text NOT NULL,
    signup_date date NOT NULL,
    active boolean NOT NULL,
    amount_usd numeric(18,2) NOT NULL,
    country character(2) NOT NULL,
    status text NOT NULL,
    discount_pct numeric(5,2),
    notes text,
    CONSTRAINT good_customers_amount_usd_check CHECK ((amount_usd >= (0)::numeric)),
    CONSTRAINT good_customers_id_check CHECK ((id >= 1)),
    CONSTRAINT good_customers_name_check CHECK ((length(name) >= 1)),
    CONSTRAINT good_customers_status_check CHECK ((status = ANY (ARRAY['active'::text, 'inactive'::text, 'prospect'::text])))
);


--
-- Name: purchases; Type: TABLE; Schema: sales; Owner: -
--

CREATE TABLE sales.purchases (
    purchase_id integer NOT NULL,
    customer_id integer NOT NULL,
    purchase_date date NOT NULL,
    item text NOT NULL,
    amount_usd numeric(18,2) NOT NULL,
    CONSTRAINT purchases_amount_usd_check CHECK ((amount_usd >= (0)::numeric))
);


--
-- Name: v_good_customers; Type: VIEW; Schema: sales; Owner: -
--

CREATE VIEW sales.v_good_customers AS
 SELECT id,
    name,
    email,
    country,
        CASE
            WHEN active THEN 'Yes'::text
            ELSE 'No'::text
        END AS active_status,
    status,
    amount_usd,
    COALESCE(discount_pct, (0)::numeric) AS discount_pct,
    round((amount_usd * ((1)::numeric - (COALESCE(discount_pct, (0)::numeric) / 100.0))), 2) AS net_amount,
    signup_date,
    notes
   FROM sales.good_customers;


--
-- Data for Name: good_customers; Type: TABLE DATA; Schema: alt; Owner: -
--

INSERT INTO alt.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (101, 'Alt Alice', 'alt.alice@example.com', '2024-04-01', true, 12.34, 'US', 'prospect', 0.00, 'ALT schema');
INSERT INTO alt.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (102, 'Alt Bob', 'alt.bob@example.com', '2024-04-02', false, 0.00, 'US', 'inactive', 0.00, 'ALT schema');


--
-- Data for Name: purchases; Type: TABLE DATA; Schema: alt; Owner: -
--

INSERT INTO alt.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (1001, 101, '2024-04-01', 'Alt Trial', 12.34);


--
-- Data for Name: good_customers; Type: TABLE DATA; Schema: sales; Owner: -
--

INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (1, 'Amy Adams', 'amy.adams@example.com', '2024-01-05', true, 19.99, 'US', 'active', 0.00, 'First purchase');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (2, 'Ben Baker', 'ben.baker@example.com', '2024-01-06', true, 49.00, 'US', 'active', 10.00, 'Loyalty promo');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (3, 'Chad Chen', 'chad.chen@example.com', '2024-01-07', false, 0.00, 'CA', 'inactive', 0.00, 'Churned');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (4, 'Dara Diaz', 'dara.diaz@example.com', '2024-01-08', true, 120.50, 'MX', 'active', 5.00, 'High value');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (5, 'Eli Evans', 'eli.evans@example.com', '2024-01-09', true, 9.99, 'US', 'prospect', 0.00, 'Trial');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (6, 'Fay Fang', 'fay.fang@example.co.uk', '2024-02-01', true, 39.95, 'GB', 'active', 0.00, '—');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (7, 'Gus Green', 'gus.green@example.com', '2024-02-02', true, 18.75, 'US', 'active', 0.00, 'Cross-sell');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (8, 'Hui Huang', 'hui.huang@example.cn', '2024-02-03', true, 77.10, 'CN', 'active', 15.00, 'Holiday sale');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (9, 'Ian Irwin', 'ian.irwin@example.ie', '2024-02-04', false, 0.00, 'IE', 'inactive', 0.00, 'Refunded');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (10, 'Jia Jin', 'jia.jin@example.sg', '2024-02-05', true, 55.55, 'SG', 'active', 0.00, 'APAC');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (11, 'Kai Kim', 'kai.kim@example.kr', '2024-03-01', true, 12.00, 'KR', 'active', 0.00, 'Addon');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (12, 'Liz Lee', 'liz.lee@example.com', '2024-03-02', true, 25.00, 'US', 'active', 0.00, 'Email opt-in');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (13, 'Max Ma', 'max.ma@example.com', '2024-03-03', true, 30.00, 'US', 'active', 20.00, 'Coupon');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (14, 'Nia Nash', 'nia.nash@example.com', '2024-03-04', true, 60.00, 'US', 'active', 0.00, 'Upsell');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (15, 'Ola O''Neil', 'ola.oneil@example.ie', '2024-03-05', true, 15.49, 'IE', 'active', 0.00, 'Name with apostrophe');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (16, 'Pia Park', 'pia.park@example.com', '2024-03-06', true, 99.99, 'US', 'active', 0.00, 'Big basket');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (17, 'Qin Qi', 'qin.qi@example.cn', '2024-03-07', true, 8.00, 'CN', 'active', 0.00, '—');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (18, 'Raj Rao', 'raj.rao@example.in', '2024-03-08', true, 22.25, 'IN', 'active', 0.00, 'India launch');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (19, 'Sue Sun', 'sue.sun@example.com', '2024-03-09', true, 11.11, 'US', 'active', 0.00, 'Round trip');
INSERT INTO sales.good_customers (id, name, email, signup_date, active, amount_usd, country, status, discount_pct, notes) VALUES (20, 'Ted Tran', 'ted.tran@example.com', '2024-03-10', true, 200.00, 'US', 'active', 0.00, 'Whale');


--
-- Data for Name: purchases; Type: TABLE DATA; Schema: sales; Owner: -
--

INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (1, 1, '2024-01-05', 'Starter Kit', 19.99);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (2, 2, '2024-01-10', 'Subscription', 49.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (3, 2, '2024-02-15', 'Addon Pack', 15.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (4, 4, '2024-01-08', 'Premium Bundle', 120.50);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (5, 5, '2024-01-12', 'Trial Extension', 9.99);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (6, 6, '2024-02-01', 'Special Edition', 39.95);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (7, 7, '2024-02-02', 'Cross-sell Pack', 18.75);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (8, 8, '2024-02-03', 'Holiday Bundle', 77.10);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (9, 10, '2024-02-05', 'APAC Subscription', 55.55);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (10, 11, '2024-03-01', 'Mobile Addon', 12.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (11, 12, '2024-03-02', 'Email Promo', 25.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (12, 13, '2024-03-03', 'Coupon Deal', 24.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (13, 13, '2024-03-15', 'Coupon Deal', 6.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (14, 14, '2024-03-04', 'Upsell Offer', 60.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (15, 15, '2024-03-05', 'IE Discount Pack', 15.49);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (16, 16, '2024-03-06', 'Large Basket', 99.99);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (17, 18, '2024-03-08', 'India Launch Pack', 22.25);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (18, 19, '2024-03-09', 'Return Trip', 11.11);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (19, 20, '2024-03-10', 'Whale Package', 200.00);
INSERT INTO sales.purchases (purchase_id, customer_id, purchase_date, item, amount_usd) VALUES (20, 20, '2024-03-20', 'Whale Addon', 150.00);


--
-- Name: good_customers good_customers_pkey; Type: CONSTRAINT; Schema: alt; Owner: -
--

ALTER TABLE ONLY alt.good_customers
    ADD CONSTRAINT good_customers_pkey PRIMARY KEY (id);


--
-- Name: purchases purchases_pkey; Type: CONSTRAINT; Schema: alt; Owner: -
--

ALTER TABLE ONLY alt.purchases
    ADD CONSTRAINT purchases_pkey PRIMARY KEY (purchase_id);


--
-- Name: good_customers good_customers_pkey; Type: CONSTRAINT; Schema: sales; Owner: -
--

ALTER TABLE ONLY sales.good_customers
    ADD CONSTRAINT good_customers_pkey PRIMARY KEY (id);


--
-- Name: purchases purchases_pkey; Type: CONSTRAINT; Schema: sales; Owner: -
--

ALTER TABLE ONLY sales.purchases
    ADD CONSTRAINT purchases_pkey PRIMARY KEY (purchase_id);


--
-- Name: purchases purchases_customer_id_fkey; Type: FK CONSTRAINT; Schema: sales; Owner: -
--

ALTER TABLE ONLY sales.purchases
    ADD CONSTRAINT purchases_customer_id_fkey FOREIGN KEY (customer_id) REFERENCES sales.good_customers(id);


--
-- PostgreSQL database dump complete
--