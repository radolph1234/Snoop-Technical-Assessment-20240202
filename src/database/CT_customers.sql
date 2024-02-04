CREATE TABLE IF NOT EXISTS customers (
    customerId uuid PRIMARY KEY,
    customerName char(252),
    lastTransaction DATE,
	meta_updated timestamp NOT NULL
);