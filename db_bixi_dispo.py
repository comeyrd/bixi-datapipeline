 
import psycopg2
from dotenv import load_dotenv
import os
from psycopg2.extras import execute_batch

load_dotenv()

def get_pg_connection():
    return psycopg2.connect(
        user=os.getenv("user"),
        password=os.getenv("password"),
        host=os.getenv("host"),
        port=os.getenv("port"),
        dbname=os.getenv("dbname")
    )   

def insert_ceiled_means(dict_rows):
    rows = [
        (
            row["station_id"],
            row["timestamp"],  # integer UNIX timestamp
            row["num_bikes_available"],
            row["num_ebikes_available"],
            row["num_bikes_disabled"],
            row["num_docks_available"],
            row["num_docks_disabled"]
        )
        for row in dict_rows
    ]

    insert_query = """
        INSERT INTO bixi_dispo (
            station_id, timestamp,
            num_bikes_available, num_ebikes_available,
            num_bikes_disabled, num_docks_available, num_docks_disabled
        )
        VALUES (
            %s,
            TO_TIMESTAMP(%s),
            %s, %s, %s, %s, %s
        )
    """

    try:
        conn = get_pg_connection()
        cur = conn.cursor()
        cur.execute("SET TIME ZONE 'America/Montreal';")
        execute_batch(cur, insert_query, rows)
        conn.commit()
        cur.close()
        conn.close()
        print("Inserted", len(rows), "rows.")
    except Exception as e:
        print("Failed to insert:", e)
