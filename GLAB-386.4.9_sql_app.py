from sqlalchemy import create_engine
from sqlalchemy import text
import pandas as pd

"""
Basic Python and Pandas program to connect to local MySQL Database
!Important! 
Make sure you install: `pip install pandas sqlalchemy mysql-connector-python`
Docs: https://docs.sqlalchemy.org/en/20/tutorial/dbapi_transactions.html
"""

#? 1. Credentials:
user = 'root' # <- update to your user (same as mysql workbench)
password = 'root' # <- update to your password (same as mysql workbench)
db = 'classicmodels' # <- update the db name 
host = 'localhost' # localhost or 127.0.0.1
port = '3306' # <- default port

#? 2. Connection URL:
# 'mysql+mysqlconnector://user:password@host:port/database'
url = f'mysql+mysqlconnector://{user}:{password}@{host}:{port}/{db}'

#? 3. Create Engine using the connection url
engine = create_engine(url)

#? 4 Read and load table to DataFrame
#q = 'SELECT * FROM `orders`' #TODO: update table_name
sql_query_order =""" SELECT orderNumber, productCode, priceEach, orderLineNumber, quantityOrdered FROM orderdetails; """
SQL_Query_product = """SELECT * FROM products"""

with engine.connect() as my_conn:
# Use pandas read_sql() to read data from the database into a Dataframe
    my_data = pd.read_sql(text(SQL_Query_product),my_conn)
	#print all records from table
    print(my_data)

    print(my_data.head(10))

    products_df = pd.read_sql(text(SQL_Query_product),my_conn,index_col ='productCode')
    print(products_df)

    print(products_df[['buyPrice','productName']].head(10))
    print("\nBasic Statistics:")
    print(products_df.describe())

    print(products_df.dtypes)
    print(products_df.shape) # Get the number of rows and columns.
    print(products_df.shape[0]) # Get the number of rows only.
    print(products_df.shape[1]) # Get the number of columns only
    print("\nMissing Values:")
    print(products_df.isnull().sum())

# Grouping and Aggregations
# Example: Group by 'productLine' and calculate the total quantityInStock and average price for each productLine

    grouped_df = products_df.groupby('productLine').agg({'quantityInStock': 'sum', 'buyPrice': 'mean'}).reset_index()
    print("\nGrouped Data:")
    print(grouped_df)

#Using Order table
    orders_prod_df = pd.read_sql(text(sql_query_order),my_conn)
    print("Sample of the 'orders' DataFrame:")
    print(orders_prod_df.head())

    orders_prod_df['totalCost'] = orders_prod_df['priceEach'] * orders_prod_df['quantityOrdered']
# Group by 'orderNumber' and sum 'totalCost' for each group
    grouped_df = orders_prod_df.groupby('orderNumber')['totalCost'].sum().reset_index()
    print(grouped_df)

    