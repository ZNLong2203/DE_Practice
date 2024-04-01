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

    # postgres_cursor.execute("CREATE SCHEMA s4_nnl")
    postgres_cursor.execute("CREATE TABLE IF NOT EXISTS s4_nnl.customer_scd_type1 (cust_id VARCHAR(255) PRIMARY KEY , cust_nm VARCHAR(255), birth_date DATE, add_id VARCHAR(255), create_date DATE)")
    postgres_conn.commit()

    # Insert the data customer
    postgres_cursor.execute("SELECT cust_id, cust_nm, birth_date, add_id, opn_dt FROM ai4e_test.customer")
    data = postgres_cursor.fetchall()
    for row in range(len(data)):
        data[row] = list(data[row])
        data[row][1] = data[row][1].replace("'", "''")
        format_data = ', '.join([f"'{val}'" if isinstance(val, str)
                       else(f"'{val}'") if isinstance(val, datetime.date) else str(val) for val in data[row]])
        postgres_cursor.execute(f"INSERT INTO s4_nnl.customer_scd_type1 (cust_id, cust_nm, birth_date, add_id, create_date) VALUES ({format_data})")
    postgres_conn.commit()

    # Update the data customer in 2023-10-18
    postgres_cursor.execute("SELECT cust_id, cust_nm, birth_date, add_id FROM ai4e_test.customer_cdc_18")
    data = postgres_cursor.fetchall()
    for row in data:
        postgres_cursor.execute(
                """
                INSERT INTO s4_nnl.customer_scd_type1 (cust_id, cust_nm, birth_date, add_id, create_date)
                VALUES (%s, %s, %s, %s, '2023-10-18')
                ON CONFLICT (cust_id) DO UPDATE
                SET cust_nm = EXCLUDED.cust_nm,
                    birth_date = EXCLUDED.birth_date,
                    add_id = EXCLUDED.add_id,
                    create_date = '2023-10-18'
                """,
                (row[0], row[1], row[2], row[3])
            )
    postgres_conn.commit()

    # Update the customer data in 2023-10-19
    postgres_cursor.execute("SELECT cust_id, cust_nm, birth_date, add_id FROM ai4e_test.customer_cdc_19")
    data = postgres_cursor.fetchall()
    for row in data:
        postgres_cursor.execute(
            """
            INSERT INTO s4_nnl.customer_scd_type1 (cust_id, cust_nm, birth_date, add_id, create_date)
            VALUES (%s, %s, %s, %s, '2023-10-19')
            ON CONFLICT (cust_id) DO UPDATE
            SET cust_nm = EXCLUDED.cust_nm,
                birth_date = EXCLUDED.birth_date,
                add_id = EXCLUDED.add_id,
                create_date = '2023-10-19'
            """,
            (row[0], row[1], row[2], row[3])
        )
    postgres_conn.commit()
except Exception as e:
    print(e)