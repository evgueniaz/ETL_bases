import psycopg2
import pandas as pd

conn_string_1 = "host='localhost' port=54320 dbname='my_database' user='root' password='postgres'"
conn_string_2 = "host='localhost' port=5433 dbname='my_database' user='root' password='postgres'"


conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY customer TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY customer from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)


conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY lineitem TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY lineitem from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY nation TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY nation from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY orders TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY orders from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY part TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY part from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY partsupp TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY partsupp from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY region TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY region from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_1
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY supplier TO STDOUT WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'w') as f:
        cursor.copy_expert(q, f)

conn_string = conn_string_2
with psycopg2.connect(conn_string) as conn, conn.cursor() as cursor:
    q = "COPY supplier from STDIN WITH DELIMITER ',' CSV HEADER;"
    with open('resultsfile.csv', 'r') as f:
        cursor.copy_expert(q, f)






conn_string= conn_string_2
with psycopg2.connect(conn_string_2) as conn, conn.cursor() as cursor:
    cursor.execute('select count(*) from customer limit 1')
    print(cursor.fetchall())

conn_string= conn_string_2
with psycopg2.connect(conn_string_2) as conn, conn.cursor() as cursor:
    cursor.execute('select count(*) from supplier limit 1')
    print(cursor.fetchall())

# print(pd.read_csv('resultsfile.csv').head())