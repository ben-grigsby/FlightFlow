# etl/postgre.py

import os, sys, json
import psycopg2


scripts_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
sys.path.append(scripts_path)

with open('infra/init_db.sql', 'r') as f:
    sql = f.read()

def insert_into_postgres(db, user, password, data, insert_quer):
    """
    Insert a single flight data record (dict) into PostgreSQL database.
    """

    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host="postgres",
        port="5432"
    )

    cur = conn.cursor()

    cur.execute(sql)

    try:
        for state in data:
            cur.execute(insert_quer, state)
        
        conn.commit()
        print("Inserted all data into database")
    
    except Exception as e:
        print(f"Failed to insert into Postgres: {e}")
    finally:
        cur.close()
        conn.close()
    

    