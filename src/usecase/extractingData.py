import psycopg2 as db
import pandas as pd
from psycopg2 import OperationalError

try:
    conn_string = "dbname='airflow' user='airflow' password='airflow' host='localhost' port='5444'"
    conn = db.connect(conn_string)
    
    query = "SELECT * FROM users"
    
    # Use read_sql_query to read a query into a DataFrame
    df = pd.read_sql_query(query, conn)
    
    # Convert the DataFrame to JSON format
    json_data = df.to_json(orient='records')
    print(json_data)

except OperationalError as e:
    print(f"Error: {e}")

finally:
    if conn:
        conn.close()
