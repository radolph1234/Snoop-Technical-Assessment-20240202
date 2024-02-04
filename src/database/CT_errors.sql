CREATE TABLE IF NOT EXISTS errors (
    UniqueHashKey VARCHAR(252) PRIMARY KEY,
    customerId uuid NOT NULL,
    transactionId uuid NOT NULL,
    transactionDate VARCHAR(252),
    sourceDate VARCHAR(252),
    merchantId SMALLINT,
    categoryId SMALLINT,         
    currency CHAR(3),   
    amount float8,      
    description VARCHAR,              
    validTransactionDate BOOL,          
    validSourceDate BOOL,       
    validCurrency BOOL,         
    duplicate BOOL,
	meta_updated timestamp NOT NULL
);