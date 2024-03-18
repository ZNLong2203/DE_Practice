import psycopg2
from psycopg2 import extras
import datetime

try:
    postgres_conn = psycopg2.connect(
        host="introduction-01-intro-ap-southeast-1-dev-introduction-db.cpfm8ml2cxp2.ap-southeast-1.rds.amazonaws.com",
        dbname="postgres",
        user="postgres",
        password="postgres123"
    )
    postgres_cursor = postgres_conn.cursor()

    postgres_cursor.execute(
        "CREATE TABLE IF NOT EXISTS s4_nnl.customer_scd_type2 (cust_id VARCHAR(255), cust_nm VARCHAR(255), birth_date DATE, add_id VARCHAR(255), opn_dt DATE, end_dt DATE, create_date DATE, update_date DATE, is_active INT, change_type VARCHAR(255))")
    postgres_conn.commit()


    def process_data(query, cnt):
        postgres_cursor.execute(query)
        data = postgres_cursor.fetchall()
        if (cnt == 1):
            formatted_data = [
                (row[0], row[1].replace("'", "''"), row[2], row[3], row[4], row[5], '2023-10-17', None, 1, 'CDC_INSERT')
                for row in data]
        elif (cnt == 2):
            postgres_cursor.executemany(
                """
                UPDATE s4_nnl.customer_scd_type2
                SET
                    update_date = %s,
                    is_active = %s,
                    change_type = %s
                WHERE cust_id = %s AND is_active = 1
                """,
                [('2023-10-18', 0, 'CDC_UPDATE', row[0]) for row in data]
            )
            postgres_conn.commit()

            formatted_data = [
                (row[0], row[1].replace("'", "''"), row[2], row[3], row[4], row[5], '2023-10-18', None, 1, 'CDC_INSERT')
                for row in data]
        elif (cnt == 3):
            postgres_cursor.executemany(
                """
                UPDATE s4_nnl.customer_scd_type2
                SET
                    update_date = %s,
                    is_active = %s,
                    change_type = %s
                WHERE cust_id = %s AND is_active = 1
                """,
                [('2023-10-19', 0, 'CDC_UPDATE', row[0]) for row in data]
            )
            postgres_conn.commit()

            formatted_data = [
                (row[0], row[1].replace("'", "''"), row[2], row[3], row[4], row[5], '2023-10-19', None, 1, 'CDC_INSERT')
                for row in data]

        extras.execute_values(
            postgres_cursor,
            """
                INSERT INTO s4_nnl.customer_scd_type2 (cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt, create_date, update_date, is_active, change_type)
                VALUES %s
            """,
            formatted_data
        )
        postgres_conn.commit()


    process_data("SELECT * FROM ai4e_test.customer", 1)
    process_data("SELECT * FROM ai4e_test.customer_cdc_18", 2)
    process_data("SELECT * FROM ai4e_test.customer_cdc_19", 3)
except Exception as e:
    print(e)
