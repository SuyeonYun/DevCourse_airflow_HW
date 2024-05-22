from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
import psycopg2

from datetime import datetime
from datetime import timedelta

import requests
import logging


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


@task
def extract(url):
    logging.info(datetime.utcnow())
    f = requests.get(url)
    return f.json()


@task
def transform(json):
    records = []
    
    for country in json:
        name = country['name']['common']
        population = country['population']
        area = country['area']
        
        records.append([name, population, area])
    logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")    
    cur = get_Redshift_connection()   
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        cur.execute(f"DELETE FROM {schema}.{table};") 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            name = r[0]
            population = r[1]
            area = r[2]
            sql = f"INSERT INTO {schema}.{table} VALUES ('{name}', '{population}', '{area}')"
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;") 
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        cur.execute("ROLLBACK;")   
    logging.info("load done")


with DAG(
    dag_id='world_info',
    start_date=datetime(2024, 5, 22),  
    tags = ['API'],
    schedule='30 6 * * SAT',  # 매주 토요일 6시 30분으로 스케줄링 하기
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
    }
) as dag:

    url = Variable.get("world_info_url")
    schema = 'yusuyeon678'   ## 자신의 스키마로 변경
    table = 'world_info'

    result = transform(extract(url))
    load(schema, table, result)