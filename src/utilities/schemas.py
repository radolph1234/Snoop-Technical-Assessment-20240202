
## Used to define the column order and types for the final dataframes
## Column order should match that of the final Postgres table 

customers = {
    'customerId': '',
    'customerName': '',
    'lastTransaction': '',
	'meta_updated': ''
}

transactions = {
    'UniqueHashKey': '',
    'customerId': '',
    'transactionId': '',
    'transactionDate': '',
    'sourceDate': '',
    'merchantId': '',
    'categoryId': '',         
    'currency': '',   
    'amount': '',      
    'description': '',              
    'validTransactionDate': '',          
    'validSourceDate': '',       
    'validCurrency': '',         
    'duplicate': '',
	'meta_updated': ''
}

errors = {
    'UniqueHashKey': '',
    'customerId': '',
    'transactionId': '',
    'transactionDate': '',
    'sourceDate': '',
    'merchantId': '',
    'categoryId': '',         
    'currency': '',   
    'amount': '',      
    'description': '',              
    'validTransactionDate': '',          
    'validSourceDate': '',       
    'validCurrency': '',         
    'duplicate': '',
	'meta_updated': ''
}

