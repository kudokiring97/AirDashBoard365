import pandas as pd
import streamlit as st
import snowflake.connector

def load_air_quality_data(serial_no=None):

    user = st.secrets["snowflake"]["user"]
    password = st.secrets["snowflake"]["password"]
    account = st.secrets["snowflake"]["account"]
    warehouse = st.secrets["snowflake"]["warehouse"]
    database = st.secrets["snowflake"]["database"]
    schema = st.secrets["snowflake"]["schema"]
   
    conn = snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
        schema=schema
    )

    query = "SELECT * FROM measurements "

    if serial_no:
        query += f" WHERE serial_no = '{serial_no}' ORDER BY date"  # serial_no 필터 추가

    # SQL 쿼리를 사용해 데이터 로드
    df = pd.read_sql_query(query, conn)
    
    conn.close()

    return df
