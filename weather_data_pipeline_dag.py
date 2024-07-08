import json
from datetime import timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Константы
CITY = "London"
API_KEY = Variable.get("OPENWEATHER_API_KEY")
URL = f"http://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}"
LOCAL_FILE_PATH = "/tmp/weather_data.json"
PROCESSED_CSV_PATH = "/tmp/processed_weather_data.csv"
PARQUET_FILE_PATH = "/tmp/weather.parquet"


def download_data():
    """
    Функция получения данных с сервиса OpenWeatherMap
    """
    response = requests.get(URL)
    if response.status_code == 200:
        weather_data = response.json()
        with open(LOCAL_FILE_PATH, "w") as file:
            json.dump(weather_data, file)
    else:
        raise Exception(
            f"Error fetching data from OpenWeatherMap API: {response.status_code}."
        )


def process_data():
    """
    Функция очистки и преобразования загруженных данных
    """
    with open(LOCAL_FILE_PATH, "r") as file:
        weather_data_loaded = json.load(file)

    # Нормализация данных JSON
    df_main = pd.json_normalize(weather_data_loaded)
    df_weather = pd.json_normalize(
        weather_data_loaded, record_path=["weather"], record_prefix="weather."
    )

    # Объединение нормализованных данных
    df = pd.concat([df_main, df_weather], axis=1).drop(columns="weather")

    # Преобразование температур из Кельвинов в Цельсии
    df["main.temp"] = df["main.temp"] - 273.15
    df["main.feels_like"] = df["main.feels_like"] - 273.15
    df["main.temp_min"] = df["main.temp_min"] - 273.15
    df["main.temp_max"] = df["main.temp_max"] - 273.15

    df.to_csv(PROCESSED_CSV_PATH, index=False)


def save_data():
    """
    Функция сохранения данных в parquet-файл
    """
    processed_df = pd.read_csv(PROCESSED_CSV_PATH)
    processed_df.to_parquet(PARQUET_FILE_PATH)


default_args = {
    "owner": "vladislav",
    "depends_on_past": False,
    "email": ["ponomarenko.vladislav@outlook.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "start_date": days_ago(1),
}

with DAG(
    "weather_data_pipeline_dag",
    default_args=default_args,
    description="A weather data pipeline DAG for Monopoly",
    # Запуск ежедневно в полночь по UTC
    # schedule_interval="0 0 * * *",  # сron выражением
    # schedule_interval="@daily", # втроенным методом

    # Запуск ежедневно в полночь по MSK (UTC+3)
    schedule_interval="0 21 * * *",
    catchup=False,
    tags=["weather"],
) as dag:
    download_data_task = PythonOperator(
        task_id="download_data", 
        python_callable=download_data
    )

    process_data_task = PythonOperator(
        task_id="process_data", 
        python_callable=process_data
    )

    save_data_task = PythonOperator(
        task_id="save_data", 
        python_callable=save_data
    )

    download_data_task >> process_data_task >> save_data_task
