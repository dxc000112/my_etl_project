# complete_project/dags/etl_dag.py
import os
import sys
from datetime import datetime, timedelta
import logging
import random

# 添加 scripts 路径到 sys.path，方便导入模块
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'scripts'))

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from load_data import load_to_gcs

# 模拟 Slack 报警（打印）
def slack_alert(context):
    task_id = context.get("task_instance").task_id
    dag_id = context.get("dag").dag_id
    print(f"[❗️提醒] DAG `{dag_id}` 中任务 `{task_id}` 执行失败！")

# 数据验证任务
def validate_data():
    logging.info("🔍 正在验证数据...")
    data = ["sample"]  # 改成空列表可模拟失败
    if not data:
        raise ValueError("❌ 数据为空，终止执行")
    logging.info("✅ 数据验证通过")

# 分支决策函数
def choose_path():
    decision = random.choice(["load_task", "stop_task"])
    logging.info(f"✨ 分支选择结果：{decision}")
    return decision

# 停止执行任务
def stop_task():
    logging.info("🛑 条件不满足，任务终止")

# DAG 参数
def_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 3, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=3),
    'on_failure_callback': slack_alert
}

# 定义 DAG
with DAG(
    dag_id="complete_etl_project",
    default_args=def_args,
    description="完整 ETL 项目 DAG",
    schedule_interval="@daily",
    catchup=False
) as dag:

    validate = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data
    )

    branch = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=choose_path
    )

    load = PythonOperator(
        task_id="load_task",
        python_callable=load_to_gcs
    )

    stop = PythonOperator(
        task_id="stop_task",
        python_callable=stop_task
    )

    validate >> branch >> [load, stop]
