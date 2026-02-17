import pandas as pd
from prefect import flow, task

@task(retries=3, retry_delay_seconds=10)
def extract_data():
    # Имитация получения данных
    return {"data": [10, 20, 30], "status": "ok"}

@task
def transform_data(raw_data):
    df = pd.DataFrame(raw_data["data"], columns=["value"])
    return df["value"].sum()

@flow(name="First Flow Example")
def main_flow(threshold: int = 40):
    print("Starting flow execution...")
    data = extract_data()
    print(f"Extracted data: {data}")
    result = transform_data(data)
    print(f"Transformed result: {result}")
    if result > threshold:
        print(f"Alert! Value {result} is above threshold!")
