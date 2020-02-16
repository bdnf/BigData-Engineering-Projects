# Data Pipelines with Airflow

A music streaming company, Sparkify, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is Apache Airflow.

They have decided to bring you into the project and expect you to create high grade data pipelines that are dynamic and built from reusable tasks, can be monitored, and allow easy backfills. They have also noted that the data quality plays a big part when analyses are executed on top the data warehouse and want to run tests against their datasets after the ETL steps have been executed to catch any discrepancies in the datasets.

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.


# Starting Airflow

To start Airflow locally, Docker is used in this repository. Feel free to improve it due to your needs:

Run the project with:
```
  ./start.sh
```

Additionally, complete and run the examples using:
```
./start.sh lesson1
```
or
```
./start.sh lesson2
```

# DAG description

The full DAG workflow looks like this, and is described in [script](/AirflowDataPipeline/dags/udac_example_dag.py)

![Runtime](/AirflowDataPipeline/dags/dag-screenshot.png)
