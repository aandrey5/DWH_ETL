import sys, os
from postgres import DataTransferPostgres
from airflow import DAG
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from postgres import table_list
from statistic import DataStatisticPostgres
from datetime import datetime
sys.path.append('/usr/local/airflow/dags')

connect = {'src': "host='172.18.0.1' port=54320 dbname='my_database' user='root' password='postgres'",
           'dest': "host='172.18.0.1' port=5433 dbname='my_database' user='root' password='postgres'",
           'meta': "host='172.18.0.1' port=54320 dbname='my_database' user='root' password='postgres'"}


DEFAULT_ARGS = {
    "owner": "Morozov",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
    dag_id="pg-data-stats",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-statistics'],
) as dag1:
    for tab in table_list(connect['dest']): # modify new
       sens_task = ExternalTaskSensor(
            task_id=f'{tab}_sens',
            external_dag_id="pg-data-flow-444",
            external_task_id=f'{tab}',
            check_existence=False,
            mode='reschedule',
            poke_interval=60 * 60,
            timeout=60 * 60 * 20,
            soft_fail=True,
            # allowed_states=[State.SUCCESS],
            retries=0,
        )  

       stat_task = DataStatisticPostgres(
            config={'table': f'public.{tab}'},
            query=f'select * from {tab}',
            task_id=f'{tab}_stat',
            source_pg_conn_str=connect['src'],
            pg_conn_str=connect['dest'],
            pg_meta_conn_str=connect['meta'],
        )  # modify

       sens_task >> stat_task


