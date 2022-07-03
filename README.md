# Airflow_dag_iris
Airflow dag with iris dataset

A simple data pipeline (dag) using apache airflow.

Case:
A table in postgres db already exists with data related to iris dataset. New data are coming with the form of csv file.

New data are read form csv with 'get_flowers_from_csv' task. Then postgres db is updated with the new data with task 'update_postgres_with_new_data'.

Also older data are read from postgres db with 'get_flowers_from_postgres'. New and old data are merged and a json file is created with 'create_unprocessed_flowers_json_file' task. In the next task 'process_and_create_processed_flowers_json_file', merged data are preprocessed (normalizing values) and a json file with processed data is created and could be used in a machine learning task.




![Screenshot from 2022-07-03 16-16-21](https://user-images.githubusercontent.com/50769254/177041490-48c731cd-50aa-4d7c-9c59-82c2eeb088fd.png)
