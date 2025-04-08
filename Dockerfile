FROM apache/airflow:2.10.4

COPY ./requirements.txt /requirements.txt

USER airflow

RUN pip install -r /requirements.txt