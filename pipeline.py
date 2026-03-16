# airflow/dags/pipeline.py
import pandas as pd

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def extract(ti):
    data = pd.read_csv("/opt/airflow/dags/IOT-temp.csv")
    ti.xcom_push(key="raw_temperature", value=data.to_json(orient="records"))


def transform(ti):
    json_data = ti.xcom_pull(task_ids="extract", key="raw_temperature")
    data = pd.read_json(json_data, orient="records")

    data = data[data["out/in"].astype(str).str.lower().str.strip() == "in"]

    data["noted_date"] = pd.to_datetime(
        data["noted_date"],
        format="%d-%m-%Y %H:%M"
    ).dt.date

    per5 = data["temp"].quantile(0.05)
    per95 = data["temp"].quantile(0.95)
    data = data[(data["temp"] >= per5) & (data["temp"] <= per95)]

    hottest_days = data.loc[
        data.groupby("noted_date")["temp"].idxmax()
    ].nlargest(5, "temp")

    coldest_days = data.loc[
        data.groupby("noted_date")["temp"].idxmin()
    ].nsmallest(5, "temp")

    result = pd.concat([coldest_days, hottest_days], axis=0)

    result = result.rename(columns={
        "room_id/id": "room_id",
        "temp": "temperature",
        "out/in": "out_in",
    })

    ti.xcom_push(key="temperature", value=result.to_json(orient="records", date_format="iso"))


def load(ti):
    json_data = ti.xcom_pull(task_ids="transform", key="temperature")
    data = pd.read_json(json_data, orient="records")

    pg_hook = PostgresHook(postgres_conn_id='hse')

    stmt = """INSERT INTO temperature (id, room_id, noted_date, temperature, out_in)
              VALUES (%s, %s, %s, %s, %s)"""

    values = [
        (
            row["id"],
            row["room_id"],
            str(row["noted_date"]),
            int(row["temperature"]),
            row["out_in"],
        )
        for _, row in data.iterrows()
    ]

    with pg_hook.get_conn() as conn:
        with conn.cursor() as cur:
            cur.executemany(stmt, values)
        conn.commit()


with DAG(dag_id="transform_practice") as dag:
    extract_data = PythonOperator(
        task_id="extract",
        python_callable=extract,
    )

    transform_data = PythonOperator(
        task_id="transform",
        python_callable=transform,
    )

    load_data = PythonOperator(
        task_id="load",
        python_callable=load,
    )

    extract_data >> transform_data >> load_data
