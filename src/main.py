import psycopg2
import pandas as pd

conn_string_1 = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
conn_string_2 = "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
tables = ['customer', 'lineitem', 'nation', 'orders', 'part', 'partsupp', 'region', 'supplier']


# conn_string = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
def dump_db(conn_string, tables):
    with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
        for table in tables:
            q = f"COPY {table} TO STDOUT WITH DELIMITER ',' CSV HEADER;"
            with open('resultsfile.csv', 'w') as f:
                cursor.copy_expert(q, f)

# conn_string = "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
def load_db(conn_string, tables):
    with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
        for table in tables:
            q = f"COPY {table} from STDIN WITH DELIMITER ',' CSV HEADER;"
            with open('resultsfile.csv', 'r') as f:
                cursor.copy_expert(q, f)


dump_db(conn_string_1, tables)
load_db(conn_string_2, tables)


# conn_string= "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"
# with psycopg2.connect(conn_string_2) as conn, conn.cursor() as cursor:
#     cursor.execute('select count(*) from customer limit 1')
#     print(cursor.fetchall())


print(pd.read_csv('resultsfile.csv').head())