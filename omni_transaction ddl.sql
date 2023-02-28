-- postgreSQL

-- dsg.omni_transaction definition

-- Drop table

-- DROP TABLE dsg.omni_transaction;

CREATE TABLE dsg.omni_transaction (
	transaction_date timestamp NOT NULL,
	channel varchar(10) NOT NULL,
	channel_transaction_id int8 NOT NULL,
	customer_id int8 NOT NULL,
	product_category varchar(255) NOT NULL,
	discount_total numeric(20, 2) NOT NULL,
	subtotal numeric(20, 2) NOT NULL,
	store_no int4 NULL,
	store_division varchar(25) NULL
) PARTITION BY RANGE (transaction_date);