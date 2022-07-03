from datetime import datetime

from airflow.decorators import dag
from airflow.providers.postgres.operators.postgres import PostgresOperator


@dag(start_date=datetime(2022, 1, 1), schedule_interval="@once", catchup=False)
def seed_postgres_db_dag():
    create_flowers_table = PostgresOperator(
        task_id="create_flowers_table",
        postgres_conn_id="postgres_conn",
        sql="""
            CREATE TABLE IF NOT EXISTS flowers_table (
                id SERIAL PRIMARY KEY,
                sepal_length TEXT NOT NULL,
                sepal_width TEXT NOT NULL,
                petal_length TEXT NOT NULL,
                petal_width TEXT NOT NULL,
                variety TEXT NOT NULL
            );
            """,
    )

    seed_flowers_table = PostgresOperator(
        task_id="seed_flowers_table",
        postgres_conn_id="postgres_conn",
        sql="""
            TRUNCATE flowers_table;
            INSERT INTO flowers_table (sepal_length, sepal_width,  petal_length, petal_width,variety)
            VALUES ('5.1', '3.5', '1.4', '0.2', 'Setosa');
            INSERT INTO flowers_table (sepal_length, sepal_width,  petal_length, petal_width,variety)
            VALUES ('4.9', '3', '1.4', '0.2', 'Setosa');
            INSERT INTO flowers_table (sepal_length, sepal_width,  petal_length, petal_width,variety)
            VALUES ('4.7', '3.2', '1.3', '0.2', 'Setosa');
            """,
    )

    create_flowers_table >> seed_flowers_table


dag = seed_postgres_db_dag()

