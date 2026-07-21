from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import subprocess

default_args = {
    "owner": "Danny",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

BASE = "/home/azureuser/ProyectoAnalisis/scripts"


def cargar_datos():
    subprocess.run(
        ["python", f"{BASE}/01_cargar_datos.py"],
        check=True
    )


def eda_inicial():
    subprocess.run(
        ["python", f"{BASE}/02_eda_inicial.py"],
        check=True
    )


def limpiar_datos():
    subprocess.run(
        ["python", f"{BASE}/03_limpiar_datos.py"],
        check=True
    )


def consolidar():
    subprocess.run(
        ["python", f"{BASE}/04_consolidar.py"],
        check=True
    )


def eda_profundo():
    subprocess.run(
        ["python", f"{BASE}/05_eda_profundo.py"],
        check=True
    )


def exportar_postgres():
    subprocess.run(
        ["python", f"{BASE}/06_exportar_postgres.py"],
        check=True
    )


with DAG(
    dag_id="favorita_pipeline",
    default_args=default_args,
    description="Pipeline ETL Corporacion Favorita con Polars",
    start_date=datetime(2026, 7, 7),
    schedule=None,
    catchup=False,
) as dag:

    t1 = PythonOperator(
        task_id="cargar_datos",
        python_callable=cargar_datos,
    )

    t2 = PythonOperator(
        task_id="eda_inicial",
        python_callable=eda_inicial,
    )

    t3 = PythonOperator(
        task_id="limpiar_datos",
        python_callable=limpiar_datos,
    )

    t4 = PythonOperator(
        task_id="consolidar",
        python_callable=consolidar,
    )

    t5 = PythonOperator(
        task_id="eda_profundo",
        python_callable=eda_profundo,
    )

    t6 = PythonOperator(
        task_id="exportar_postgres",
        python_callable=exportar_postgres,
    )

    t1 >> t2 >> t3 >> t4 >> t5 >> t6
