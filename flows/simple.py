import pandas as pd
from prefect import flow, task, get_run_logger


@task(retries=3, retry_delay_seconds=10)
def extract():
    return {"data": [10, 20, 40, 70], "status": "ok"}


@task(retries=2, retry_delay_seconds=5)
def transform(raw_data):
    df = pd.DataFrame(raw_data["data"], columns=["value"])
    return df["value"].sample(1).iloc[0]


@task
def load(result):
    print(f"Data loaded successfully with result: {result}")
    return result


@flow(name="First Flow Example", log_prints=True)
def main(threshold: int = 40):
    logger = get_run_logger()
    logger.info(f"Flow started with threshold: {threshold}")

    # Extract
    logger.info("Starting extract data...") 
    data = extract()
    logger.info("Data extracted successfully")
    
    # Transform
    logger.info("Starting data transformation...")
    data_transformed = transform(data)
    logger.info("Data transformed successfully")

    # Load
    logger.info("Starting data load...")
    result = load(data_transformed)
    logger.info("Data loaded successfully")

    if result > threshold:
        logger.warning(f"Alert! Value {result} is above threshold!")

if __name__ == "__main__":
    main()
