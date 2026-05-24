# Airflow BashOperator Pipeline for E-commerce Sales Reporting

## Introduction

This project is a continuation of a previous implementation
([Automated_Data_Pipeline_Airflow_AWS](https://github.com/soichi-berson/Automated_Data_Pipeline_Airflow_AWS)),
in which Airflow and AWS were used to automate a data pipeline for
e-commerce sales reporting. In this iteration, two key architectural
changes were introduced: the TaskFlow API was replaced with the
BashOperator, and multiple individual Python files were consolidated
into a single class-based module.

## Data Source

Dataset: [E-commerce Sales Dataset (Kaggle)](https://www.kaggle.com/datasets/sharmajicoder/e-commerce-sales-dataset)

## Objective

The objective of this project is to explore and evaluate the
architectural benefits of using the BashOperator over the TaskFlow API,
and to assess the advantages of consolidating pipeline logic into a
single class-based module.

## Features

- Automated ETL pipeline using Apache Airflow with BashOperator
- Single class-based module consolidating all pipeline logic
- Weekly KPI reporting (Revenue, Orders, AOV, WoW growth)
- Data visualisation using Matplotlib
- PDF report generation
- AWS S3 integration for data ingestion and storage
- Environment-based configuration following the Twelve-Factor App methodology

## Documentation

For a detailed explanation of the design decisions and justification,
please refer to the full project report:

- `Automated_E-Commerce_Sales_Reporting_Pipeline.pdf`

## Material

- `amazon_daily_class.py`: Single class-based module consolidating all
  pipeline logic including loading, cleaning, analysing, and generating
  the PDF report
- `amazon_sales_reporting_dag_bash.py`: Airflow DAG definition using
  BashOperator
- `Automated_E-Commerce_Sales_Reporting_Pipeline.pdf`: Full project report


## Related Project

- Previous implementation using TaskFlow API:
  [Automated_Data_Pipeline_Airflow_AWS](https://github.com/soichi-berson/Automated_Data_Pipeline_Airflow_AWS)

## License

This project is licensed under the MIT License.

## Author

Soichiro Tanabe

Feel free to explore the project and reach out if you have any questions.
