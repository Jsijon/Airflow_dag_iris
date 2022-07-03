
from datetime import datetime
from operator import imod
from pathlib import Path

import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator


FILES_BASE_PATH = "/opt/airflow/files"
FLOWERS_CSV_FILENAME = "iris.csv"
UNPROCESSED_JSON_FILENAME = "unprocessed_iris_data" 
PROCESSED_JSON_FILENAME = "processed_iris_data"


def _update_postgres_with_new_data(ti=None):

  
    new_flower_lists = ti.xcom_pull(
            task_ids=["get_flowers_from_csv"]
    )
    # Flatten list of lists
    new_flowers = [flower for flowers_list in new_flower_lists for flower in flowers_list]
    df = pd.DataFrame(new_flowers)
    
    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()

    for index, row in df.iterrows():
        sql = "INSERT INTO flowers_table (sepal_length,sepal_width,petal_length,petal_width,variety) VALUES (%s,%s,%s,%s,%s);"
        values = (str(row.sepal_length), str(row.sepal_width), str(row.petal_length), str(row.petal_width), row.variety)
        cursor.execute(sql, values)

    pg_conn.commit()


def _get_flowers_from_postgres():

    pg_hook = PostgresHook(postgres_conn_id="postgres_conn")
    pg_conn = pg_hook.get_conn()
    cursor = pg_conn.cursor()
    cursor.execute("SELECT * FROM flowers_table;")
    users = cursor.fetchall()
    return users

def  _process_and_create_processed_flowers_json_file():

    df = pd.read_json(f"{FILES_BASE_PATH}/{UNPROCESSED_JSON_FILENAME}")

    df_max_scaled = df.copy()
    for column in ['sepal_length','sepal_width','petal_length','petal_width']:

        df_max_scaled[column] = df_max_scaled[column].astype(int)
        df_max_scaled[column] = df_max_scaled[column]  / df_max_scaled[column].abs().max()

    df_max_scaled.to_json(f"{FILES_BASE_PATH}/{PROCESSED_JSON_FILENAME}", orient="records")

@dag(start_date=datetime(2022, 1, 1), schedule_interval="@daily", catchup=False)
def process_flowers_dag():
    @task
    def get_flowers_from_csv():
        path_to_csv = f"{FILES_BASE_PATH}/{FLOWERS_CSV_FILENAME}"
        if not Path(path_to_csv).is_file():
            raise ValueError(f"{FLOWERS_CSV_FILENAME} file is missing.")

        df = pd.read_csv(path_to_csv,sep=',',header=0)
        # df = df.applymap(str)
        return df.to_dict(orient="record")

    get_flowers_from_postgres = PythonOperator(
        task_id="_get_flowers_from_postgres", python_callable=_get_flowers_from_postgres
    )

    @task
    def create_unprocessed_flowers_json_file(ti=None):
        flower_lists = ti.xcom_pull(
            task_ids=["get_flowers_from_csv", "_get_flowers_from_postgres"]
        )
        # Flatten list of lists
        flowers = [flower for flowers_list in flower_lists for flower in flowers_list]
        df = pd.DataFrame(flowers)

        df.to_json(f"{FILES_BASE_PATH}/{UNPROCESSED_JSON_FILENAME}", orient="records")


    process_and_create_processed_flowers_json_file = PythonOperator(
        task_id="_process_and_create_processed_flowers_json_file", python_callable=_process_and_create_processed_flowers_json_file
    )   

    update_postgres_with_new_data = PythonOperator(
        task_id="_update_postgres_with_new_data", python_callable=_update_postgres_with_new_data
    )  
   

   
    [get_flowers_from_csv(), get_flowers_from_postgres] >> create_unprocessed_flowers_json_file() >> process_and_create_processed_flowers_json_file #>> seed_flowers_table

    get_flowers_from_csv() >> update_postgres_with_new_data



dag = process_flowers_dag()