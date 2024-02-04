CREATE TABLE IF NOT EXISTS transactions (
    UniqueHashKey VARCHAR(252) PRIMARY KEY,
    customerId uuid NOT NULL,
    transactionId uuid NOT NULL,
    transactionDate DATE NOT NULL,
    sourceDate TIMESTAMP NOT NULL,
    merchantId SMALLINT NOT NULL,
    categoryId SMALLINT NOT NULL,         
    currency CHAR(3) NOT NULL,   
    amount float8 NOT NULL,      
    description VARCHAR,              
    validTransactionDate BOOL NOT NULL,          
    validSourceDate BOOL NOT NULL,       
    validCurrency BOOL NOT NULL,         
    duplicate BOOL NOT NULL,
	meta_updated timestamp NOT NULL
);