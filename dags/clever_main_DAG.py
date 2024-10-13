from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from scripts.clever_main_pipeline import (
    create_postgres_schema,
    create_transformed_postgres_table,
    transform_postgres_data,
    upload_csv_to_postgres,
)

default_args = {
    "owner": "bruna.calistrate",
    "start_date": datetime(2024, 10, 1),
}

datasets = (
    "company_profiles_google_maps.csv",
    "customer_reviews_google.csv",
    "fmcsa_companies.csv",
    "fmcsa_company_snapshot.csv",
    "fmcsa_complaints.csv",
    "fmcsa_safer_data.csv",
)
transformed_tables = {
    "fmcsa_companies": "analytics",
    "fmcsa_companies_complaints": "analytics",
    "google_maps_companies": "analytics",
    "google_maps_companies_reviews": "analytics",
    "transformed_fmcsa_companies": "staging",
    "transformed_google_maps_companies": "staging",
}

with DAG(
    "clever_main_DAG",
    default_args=default_args,
    catchup=False,
    schedule_interval="20 0 * * *",
    max_active_runs=3,
) as dag:
    start_task = EmptyOperator(task_id="Start", dag=dag)
    finish_task = EmptyOperator(task_id="Finish", dag=dag)
    create_staging_schema_task = PythonOperator(
        task_id="create_staging_schema",
        python_callable=create_postgres_schema,
        dag=dag,
        op_kwargs={"schema_name": "staging"},
    )
    create_analytics_schema_task = PythonOperator(
        task_id="create_analytics_schema",
        python_callable=create_postgres_schema,
        dag=dag,
        op_kwargs={"schema_name": "analytics"},
    )

    transform_tasks = {}
    transform_tables_tasks = {}

    for file in datasets:
        file_without_extension = file.split(".")[0]

        extract_to_postgres_task = PythonOperator(
            task_id=f"extract_to_postgres_{file_without_extension}",
            python_callable=upload_csv_to_postgres,
            dag=dag,
            op_kwargs={"file_name": file, "schema_name": "public"},
        )
        transform_to_postgres_task = PythonOperator(
            task_id=f"transform_{file_without_extension}",
            python_callable=transform_postgres_data,
            dag=dag,
            op_kwargs={"file_name": file, "schema_name": "staging"},
        )

        transform_tasks[file_without_extension] = transform_to_postgres_task

        (
            start_task
            >> extract_to_postgres_task
            >> create_staging_schema_task
            >> transform_to_postgres_task
        )

    for table, schema in transformed_tables.items():
        create_transform_tables_tasks = PythonOperator(
            task_id=f"create_{table}",
            python_callable=create_transformed_postgres_table,
            dag=dag,
            op_kwargs={
                "table_name": table,
                "schema_name": schema,
            },
        )

        transform_tables_tasks[table] = create_transform_tables_tasks

    (
        transform_tasks["fmcsa_companies"],
        transform_tasks["fmcsa_company_snapshot"],
        transform_tasks["fmcsa_safer_data"],
    ) >> transform_tables_tasks["transformed_fmcsa_companies"]

    (
        transform_tasks["company_profiles_google_maps"]
        >> transform_tables_tasks["transformed_google_maps_companies"]
    )

    (
        transform_tasks["fmcsa_complaints"],
        transform_tables_tasks["transformed_fmcsa_companies"],
        transform_tasks["customer_reviews_google"],
        transform_tables_tasks["transformed_google_maps_companies"],
    ) >> create_analytics_schema_task

    (
        create_analytics_schema_task
        >> (
            transform_tables_tasks["fmcsa_companies"],
            transform_tables_tasks["fmcsa_companies_complaints"],
            transform_tables_tasks["google_maps_companies"],
            transform_tables_tasks["google_maps_companies_reviews"],
        )
        >> finish_task
    )
