FROM apache/airflow:slim-2.9.1-python3.12

COPY requirements.txt /requirements.txt

RUN pip install --no-cache-dir -r /requirements.txt

USER root

RUN apt-get update && apt-get install -y \
    wget

USER airflow

ENTRYPOINT [ "/bin/bash", "-c" ]