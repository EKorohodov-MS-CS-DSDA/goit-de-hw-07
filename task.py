from airflow import DAG
from datetime import datetime
from airflow.providers.standard.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.common.sql.sensors.sql import SqlSensor

from airflow.task.trigger_rule import TriggerRule
from airflow.utils.state import State
import random
import time

medals = ['Gold', 'Silver', 'Bronze']
CONNECTION_NAME = 'goit_mysql_db'
SLEEP_SECONDS = 30
TABLE_NAME = 'olympic_dataset.medal_counts'

def mark_dag_success(**kwargs):
    dag_run = kwargs['dag_run']
    dag_run.set_state(State.SUCCESS)

def make_create_table_task():
    return SQLExecuteQueryOperator(
        task_id='create_table',
        conn_id=CONNECTION_NAME,
        sql=f"""
            CREATE DATABASE IF NOT EXISTS olympic_dataset;
            CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                medal_type VARCHAR(10),
                count INT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            """
    )

def make_pick_medal_task():
    return BranchPythonOperator(
        task_id='pick_medal_task',
        python_callable=lambda: f"insert_{random.choice(medals)}_task"
    )

def make_insert_task(medal_type):
    return SQLExecuteQueryOperator(
        task_id=f'insert_{medal_type}_task',
        conn_id=CONNECTION_NAME,
        sql=f"""
            INSERT INTO {TABLE_NAME} (medal_type, count)
            SELECT '{medal_type}', COUNT(*)
            FROM olympic_dataset.athlete_event_results
            WHERE medal = '{medal_type}';
        """
    )

def make_delay_task():
    return PythonOperator(
        task_id='generate_delay',
        python_callable=lambda: time.sleep(SLEEP_SECONDS),
        trigger_rule=TriggerRule.ONE_SUCCESS
    )

def make_correctness_check():
    return SqlSensor(
        task_id='check_for_correctness',
        conn_id=CONNECTION_NAME,
        sql=f"""
            SELECT TIMESTAMPDIFF(SECOND, created_at, NOW()) < 30
            FROM {TABLE_NAME}
            ORDER BY created_at DESC
            LIMIT 1;
        """,
        mode='poke',
        poke_interval=5,
        timeout=60
    )


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2026, 5, 6),
}

# Створюємо DAG
with DAG(
    dag_id='hw7_ekorohodov', 
    default_args=default_args, 
    schedule=None,
    catchup=False,
    tags=['ekorohodov']
) as dag:

    #1. Створює таблицю використовуючи IF_NOT_EXISTS (створення схеми додано в цю ж задачу)
    create_table = make_create_table_task()

    # 2. Випадково обирає одне зі значень ['Gold', 'Silver', 'Bronze']
    pick_medal_task = make_pick_medal_task()

    # 3. Залежно від обраного значення запускає одне із трьох завдань (розгалуження):
    calc_Bronze = make_insert_task('Bronze')
    calc_Silver = make_insert_task('Silver')
    calc_Gold = make_insert_task('Gold')

    # 4. Запускає затримку виконання наступного завдання
    generate_delay = make_delay_task()

    # 6. Перевіряє, чи найновіший запис у таблиці, створеній на етапі 1, не старший за 30 секунд (порівнюючи з поточним часом).
    check_for_correctness = make_correctness_check()

    create_table >> pick_medal_task
    pick_medal_task >> [calc_Bronze, calc_Silver, calc_Gold]
    [calc_Bronze, calc_Silver, calc_Gold] >> generate_delay
    generate_delay >> check_for_correctness
