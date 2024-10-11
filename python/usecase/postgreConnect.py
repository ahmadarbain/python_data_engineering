import psycopg2 as db
from psycopg2 import OperationalError
from faker import Faker

try:
    conn_string = "dbname='airflow' user='airflow' password='airflow' host='localhost' port='5444'"
    conn = db.connect(conn_string)
    cur = conn.cursor()

    fake = Faker()
    data = []
    i = 2

    for r in range(1000):
        data.append((i, fake.name(), fake.street_address(),
                     fake.city(), fake.zipcode()))
        i+=1
    
    data_for_db = tuple(data)

    query =  "insert into users (id, name, street, city, zip) values(%s,%s,%s,%s,%s)"
    print(cur.mogrify(query,data_for_db[1]))

    cur.executemany(query,data_for_db)
    conn.commit()

except OperationalError as e:
    print(f"Error: {e}")
