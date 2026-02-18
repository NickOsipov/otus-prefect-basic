from flows.simple import main

DEPLOY_CONFIG = {
    "name": "demo-deployment",
    "work_pool_name": "local-pool",
    "cron": "0 * * * *",
    "parameters": {"threshold": 50},
}

if __name__ == "__main__":
    main.from_source(
        source="https://github.com/NickOsipov/otus-prefect-basic.git",
        entrypoint="flows/simple.py:main"
    ).deploy(**DEPLOY_CONFIG)
