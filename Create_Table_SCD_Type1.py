import psycopg2
from psycopg2 import extras
import datetime

# Connect to Postgres
try:
    postgres_conn = psycopg2.connect(
        host="introduction-01-intro-ap-southeast-1-dev-introduction-db.cpfm8ml2cxp2.ap-southeast-1.rds.amazonaws.com",
        dbname="postgres",
        user="postgres",
        password="postgres123"
    )
    postgres_cursor = postgres_conn.cursor()

    # Create table if not exists
    # postgres_cursor.execute("CREATE SCHEMA s4_nnl")
    postgres_cursor.execute("CREATE TABLE IF NOT EXISTS s4_nnl.customer_scd_type1 (cust_id VARCHAR(255) PRIMARY KEY , cust_nm VARCHAR(255), birth_date DATE, add_id VARCHAR(255), opn_dt DATE, end_dt DATE, create_date DATE)")
    postgres_conn.commit()

    # Define a function to process data and execute SQL
    def process_data(query, cnt):
        postgres_cursor.execute(query)
        data = postgres_cursor.fetchall()
        if(cnt == 1):
            formatted_data = [(row[0], row[1].replace("'", "''"), row[2], row[3], row[4], row[5], '2023-10-17') for row in data]
        elif (cnt == 2):
            formatted_data = [(row[0], row[1].replace("'", "''"), row[2], row[3], row[4], row[5], '2023-10-18') for row in data]
        elif (cnt == 3):
            formatted_data = [(row[0], row[1].replace("'", "''"), row[2], row[3], row[4], row[5], '2023-10-19') for row in data]
        extras.execute_values(
            postgres_cursor,
            """
            INSERT INTO s4_nnl.customer_scd_type1 (cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt, create_date)
            VALUES %s
            ON CONFLICT (cust_id) DO UPDATE
            SET cust_nm = EXCLUDED.cust_nm,
                birth_date = EXCLUDED.birth_date,
                add_id = EXCLUDED.add_id,
                opn_dt = EXCLUDED.opn_dt,
                end_dt = EXCLUDED.end_dt,
                create_date = EXCLUDED.create_date
            """,
            formatted_data
        )
        postgres_conn.commit()

    # Process the data
    process_data("SELECT cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt FROM ai4e_test.customer", 1)
    process_data("SELECT cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt FROM ai4e_test.customer_cdc_18", 2)
    process_data("SELECT cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt FROM ai4e_test.customer_cdc_19", 3)

except Exception as e:
    print(e)
