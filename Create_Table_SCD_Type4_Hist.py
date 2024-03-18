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

    # Create table SCD type4 history
    postgres_cursor.execute("CREATE TABLE IF NOT EXISTS s4_nnl.customer_scd_type4_hist AS SELECT * FROM ai4e_test.customer")
    postgres_cursor.execute("ALTER TABLE s4_nnl.customer_scd_type4_hist ADD COLUMN create_time DATE DEFAULT '2023-10-17', ADD COLUMN update_time DATE, ADD COLUMN change_type VARCHAR(255) DEFAULT 'CDC_INSERT'")
    postgres_conn.commit()

    # Insert and update data 2023-10-18
    postgres_cursor.execute("UPDATE s4_nnl.customer_scd_type4_hist AS c_17 \
                            SET cust_nm = c_18.cust_nm, birth_date = c_18.birth_date, add_id = c_18.add_id, opn_dt = c_18.opn_dt, end_dt = c_18.end_dt, update_time = '2023-10-18', change_type = 'CDC_UPDATE' \
                            FROM ai4e_test.customer_cdc_18 AS c_18 \
                            WHERE c_17.cust_id = c_18.cust_id AND c_17.update_time IS NULL")
    postgres_cursor.execute("INSERT INTO s4_nnl.customer_scd_type4_hist (cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt, create_time, update_time, change_type) \
                            SELECT cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt, '2023-10-18', NULL, 'CDC_INSERT' \
                            FROM ai4e_test.customer_cdc_18 ")
    postgres_conn.commit()

    # Insert and update data 2023-10-19
    postgres_cursor.execute("UPDATE s4_nnl.customer_scd_type4_hist AS c_17 \
                            SET cust_nm = c_19.cust_nm, birth_date = c_19.birth_date, add_id = c_19.add_id, opn_dt = c_19.opn_dt, end_dt = c_19.end_dt, update_time = '2023-10-19', change_type = 'CDC_UPDATE' \
                            FROM ai4e_test.customer_cdc_19 AS c_19 \
                            WHERE c_17.cust_id = c_19.cust_id AND c_17.update_time IS NULL")
    postgres_cursor.execute("INSERT INTO s4_nnl.customer_scd_type4_hist (cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt, create_time, update_time, change_type) \
                            SELECT cust_id, cust_nm, birth_date, add_id, opn_dt, end_dt, '2023-10-19', NULL, 'CDC_INSERT' \
                            FROM ai4e_test.customer_cdc_19 ")
    postgres_conn.commit()
except Exception as e:
    print(e)
