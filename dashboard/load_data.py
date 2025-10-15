# dashboard/load_data.py

import pandas as pd
from sqlalchemy import create_engine

def get_gold_table(limit=100):
    engine = create_engine("postgresql://user:pass@postgres:5432/flight_db")
    query = f"""
        SELECT * 
        FROM opensky.gold_flight_stats_by_country
        LIMIT {limit}
    """
    df = pd.read_sql(query, engine)
    
    return df


def get_top_origin_countries():
    engine = engine = create_engine("postgresql://user:pass@postgres:5432/flight_db")
    query = f"""
        SELECT 
            *
        FROM opensky.gold_top_origin_countries
    """
    df = pd.read_sql(query, engine)

    return df