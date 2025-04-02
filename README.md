# Dagster Testing
## Summary
I started running test cases for an asset based ETL Orchestrator called [Dagster](https://dagster.io/). Dagster allows data engineers to build ETL Pipelines while considering each process step as an asset. This allows us to cache preprocessing and other data artifacts while providing rich process diagrams and log files like the below cleaning process from my fidelity brokerage stock screener.

## Process Flow
![alt_text](https://github.com/amason445/dagster-testing/blob/main/stock_load_process.png)

## Run History
![alt_text](https://github.com/amason445/dagster-testing/blob/main/stock_load_runs.png)

## Exception Handling
![alt_text](https://github.com/amason445/dagster-testing/blob/main/stock_load_bugs.png)
