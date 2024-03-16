import psycopg2
import pyodbc
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

    postgres_cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = 'ai4e_test' AND table_name = 'einvoice'")
    columns = [headers[3] for headers in postgres_cursor.fetchall()]
    postgres_cursor.execute("SELECT * FROM information_schema.columns WHERE table_schema = 'ai4e_test' AND table_name = 'einvoice'")
    datatype = [headers[7] for headers in postgres_cursor.fetchall()]

    for i in range(len(datatype)):
        if datatype[i] == 'character varying':
            datatype[i] = 'VARCHAR(255)'
        elif datatype[i] == 'numeric':
            datatype[i] = 'INT'
        elif datatype[i] == 'timestamp without time zone':
            datatype[i] = 'DATETIME2'
    # print(datatype)
    data = postgres_cursor.execute("SELECT * FROM ai4e_test.einvoice t LIMIT 10")
    # print(columns)
except Exception as e:
    print(e)

# Connect to SQL Server
try:
    sql_server_conn = pyodbc.connect(
        DRIVER='{SQL Server}',
        SERVER='DESKTOP-L6QA10P\SQLEXPRESS',
        DATABASE='Ai4e_test',
        Trusted_Connection='yes'
    )

    sql_server_cursor = sql_server_conn.cursor()

    if columns:
        # Create table in SQL Server
        sql_server_cursor.execute("DROP TABLE IF EXISTS dbo.einvoice_test")
        sql_server_cursor.execute("CREATE TABLE einvoice_test (id INT PRIMARY KEY IDENTITY(1,1), " + ", ".join([f'{col} {dtype}' for col, dtype in zip(columns, datatype)]) + ")")

        # Insert data from Postgres to SQL Server
        for row in postgres_cursor.fetchall():
            # print(row)
            format_data = ', '.join([f"'{val}'" if isinstance(val, str)
                                     else (f"'{val}'" if isinstance(val, datetime.datetime) else str(val)) for val in row])
            sql_server_cursor.execute(f"INSERT INTO dbo.einvoice_test ({', '.join(columns)}) VALUES ({format_data})")
        sql_server_conn.commit()
    else:
        print("No columns found in the PostgreSQL table.")
except Exception as e:
    print(e)
