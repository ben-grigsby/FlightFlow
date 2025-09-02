import psycopg2

conn = psycopg2.connect(
    dbname="flight_db",
    user="user",
    password="pass",
    host="postgres",
    port="5432"
)

cur = conn.cursor()

print("Starting query")

select_quer = """
SELECT origin_country FROM flight_data LIMIT 5;
"""

cur.execute(select_quer)

rows = cur.fetchall()

print(len(rows))

for i, row in enumerate(rows):
    print(f"Printing row: {i}")
    print(row)

cur.close()
conn.close()