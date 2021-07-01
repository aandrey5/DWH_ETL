from airflow import DAG
from postgres import DataTransferPostgres
from datetime import datetime
from test_stat import connect

import sys, os
sys.path.append('/usr/local/airflow/dags')



DEFAULT_ARGS = {
    "owner": "Morozov",
    "start_date": datetime(2021, 1, 25),
    "retries": 1,
    "email_on_failure": False,
    "email_on_retry": False,
    "depends_on_past": True,
}

with DAG(
    dag_id="pg-data-flow-444",
    default_args=DEFAULT_ARGS,
    schedule_interval="@daily",
    max_active_runs=1,
    tags=['data-flow'],
) as dag1:
    t1 = DataTransferPostgres(
        config={'table': 'public.customer'},
        query='select * from customer limit 500000',
        task_id='customer',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str = connect['meta'],
    )


    t2 = DataTransferPostgres(
        config={'table': 'public.lineitem'},
        query='select * from lineitem limit 500000',
        task_id='lineitem',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )

    t3 = DataTransferPostgres(
        config={'table': 'public.nation'},
        query='select * from nation limit 500000',
        task_id='nation',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )

    
    t4 = DataTransferPostgres(
        config={'table': 'public.orders'},
        query='select * from orders limit 500000',
        task_id='orders',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )

    t5 = DataTransferPostgres(
        config={'table': 'public.part'},
        query='select * from part limit 500000',
        task_id='part',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )


    t6 = DataTransferPostgres(
        config={'table': 'public.partsupp'},
        query='select * from partsupp limit 500000',
        task_id='partsupp',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )

    t7 = DataTransferPostgres(
        config={'table': 'public.region'},
        query='select * from region limit 500000',
        task_id='region',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )


    t8 = DataTransferPostgres(
        config={'table': 'public.supplier'},
        query='select * from supplier limit 500000',
        task_id='supplier',
        source_pg_conn_str=connect['src'],
        pg_conn_str=connect['dest'],
        pg_meta_conn_str=connect['meta'],
    )
