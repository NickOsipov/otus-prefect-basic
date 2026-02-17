import pandas as pd
from prefect import flow, task, get_run_logger

@task(retries=3, retry_delay_seconds=10)
def extract_data():
    return {"data": [10, 20, 30], "status": "ok"}

@task
def transform_data(raw_data):
    df = pd.DataFrame(raw_data["data"], columns=["value"])
    return df["value"].sum()

@flow(name="First Flow Example")
def main_flow(threshold: int = 40):
    logger = get_run_logger()
    logger.info(f"Flow started with threshold: {threshold}")
    
    logger.info("Starting extract data...") 
    data = extract_data()
    logger.info("Data transformed successfully")
    result = transform_data(data)
    if result > threshold:
        logger.warning(f"Alert! Value {result} is above threshold!")