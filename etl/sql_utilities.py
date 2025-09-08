# etl/sql_utilities.py

import psycopg2

# ==================================================================
# Utility Functions
# ==================================================================

def connect_to_db(db, user, password):
    """
    Creates the connector linked to a specified database
    """

    conn = psycopg2.connect(
        dbname=db,
        user=user,
        password=password,
        host="postgres",
        port="5432"
    )

    cur = conn.cursor()

    return cur, conn



def try_except_wrapper(fn, conn, *args, key_name=""):
    try:
        return fn(*args)
    except Exception as e:
        conn.rollback()
        print(f"[ERROR] Failed to insert {key_name}: {e}")
        return None