#!/bin/sh

DAG=dags
PLUGINS=plugins
case $1 in
  lesson1)
  echo "Starting lesson 1"
  DAG=dags-basic-pipelines
  PLUGINS=plugins
  ;;
  lesson2)
  echo "Starting lesson 2"
  DAG=dags-data-quality
  PLUGINS=plugins
  ;;
  lesson3)
  echo "Starting lesson 3: Custom Operators"
  DAG=operators-and-sub-dags/dags
  PLUGINS=operators-and-sub-dags/plugins
  ;;
  project)
  echo "Building Airflow DAG final broject"
  DAG=dags
  PLUGINS=plugins
  ;;
  stop)
  echo "Stopping the container"
  docker stop airflow
  exit 0
  ;;
  *)
  echo "Building Airflow DAG final broject"
  ;;
esac

echo "Building container"
docker pull puckel/docker-airflow
docker build --rm --build-arg AIRFLOW_DEPS="datadog,dask" --build-arg PYTHON_DEPS="flask_oauthlib>=0.9" -t puckel/docker-airflow .

docker stop airflow && docker rm airflow

echo "Starting Airflow container"

docker run -d --name airflow -p 8080:8080 -v ${PWD}/${DAG}:/usr/local/airflow/dags -v ${PWD}/${PLUGINS}:/usr/local/airflow/plugins puckel/docker-airflow webserver


echo "Now open localhost:8080 in you browser to access Airflow Web UI"
