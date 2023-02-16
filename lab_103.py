from prefect.blocks.system import Secret
from prefect import task, flow
import requests
from prefect.tasks import task_input_hash
from datetime import timedelta, datetime
from typing import List

secret_block = Secret.load("alphavantage")
API_KEY = secret_block.get()
BASE_URL = "https://www.alphavantage.co/"


@task(
    retries=3,
    retry_delay_seconds=60,
    cache_key_fn=task_input_hash,
    cache_expiration=timedelta(days=1),
)
def get_data(api_key: str, ticker: str) -> None:
    url = (
        f"{BASE_URL}/query?function=TIME_SERIES_WEEKLY&symbol={ticker}&apikey={api_key}"
    )
    r = requests.get(url)
    data = r.json()
    return data


@task()
def transform_data(data: dict) -> None:
    time_series = data["Weekly Time Series"]
    latest_week = max(time_series.keys())
    latest_week_data = time_series[latest_week]
    print(f"latest_week: {latest_week_data}")
    return latest_week_data


@flow()
def transformed_data(ticker: str = "RENT") -> None:
    data = get_data(api_key=API_KEY, ticker=ticker)
    f_data = transform_data(data)


if __name__ == "__main__":
    main_flow_state = transformed_data()
