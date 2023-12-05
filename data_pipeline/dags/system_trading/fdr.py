import datetime as dt
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from system_trading.controller.db import get_db_engine
from system_trading.api_loader.fdr import get_fdr_loader

"""
CFG
"""
FDR_CFG = {
    "start": "20200101",
    "end": dt.date.today().strftime("%Y%m%d"),
    # "end": (dt.date.today() - dt.timedelta(days=3)).strftime("%Y%m%d"),
}

db_engine = get_db_engine()

default_args = {
    "owner": "system_trading",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
}

# DAG 인스턴스 생성
dag = DAG(
    "system_trading_fdr",
    default_args=default_args,
    schedule_interval="0 17 * * 1-5",  # 평일 17시
    catchup=False,
)


# dart_fss
def check_fdr_loader_obj(**kwargs):
    fdr_loader = get_fdr_loader()
    return None


_check_fdr_loader_obj = PythonOperator(
    task_id="check_fdr_loader_obj",
    python_callable=check_fdr_loader_obj,
    provide_context=True,
    dag=dag,
)


def get_fdr_info(**kwargs):
    fdr_loader = get_fdr_loader()
    fdr_info_df = fdr_loader.get_krx_df()
    ti = kwargs["ti"]
    ti.xcom_push(key="fdr_info_df", value=fdr_info_df)
    ti.xcom_push(key="stockcodes", value=set(fdr_info_df["Code"]))
    return None


_get_fdr_info = PythonOperator(
    task_id="get_fdr_info",
    python_callable=get_fdr_info,
    provide_context=True,
    dag=dag,
)


def get_fdr_ohlcv(**kwargs):
    ti = kwargs["ti"]
    stockcodes = ti.xcom_pull(task_ids="get_fdr_info", key="stockcodes")
    fdr_loader = get_fdr_loader()
    fdr_ohlcv_df = fdr_loader.get_stocks_ohlcv_df(StockCodes=stockcodes, start=FDR_CFG["start"], end=FDR_CFG["end"])
    ti.xcom_push(key="fdr_ohlcv_df", value=fdr_ohlcv_df)
    return None


_get_fdr_ohlcv = PythonOperator(
    task_id="get_fdr_ohlcv",
    python_callable=get_fdr_ohlcv,
    provide_context=True,
    dag=dag,
)


def print_fdr_ohlcv(**kwargs):
    ti = kwargs["ti"]
    fdr_ohlcv_df = ti.xcom_pull(task_ids="get_fdr_ohlcv", key="fdr_ohlcv_df")
    print(fdr_ohlcv_df)
    return None


_print_fdr_ohlcv = PythonOperator(
    task_id="print_fdr_ohlcv",
    python_callable=print_fdr_ohlcv,
    provide_context=True,
    dag=dag,
)


def fdr_ohlcv_2db(**kwargs):
    ti = kwargs["ti"]
    fdr_ohlcv_df = ti.xcom_pull(task_ids="get_fdr_ohlcv", key="fdr_ohlcv_df")
    fdr_ohlcv_df.to_sql(name="fdr_ohlcv_df", con=db_engine, if_exists="replace", index=False)
    return None


_fdr_ohlcv_2db = PythonOperator(
    task_id="fdr_ohlcv_2db",
    python_callable=fdr_ohlcv_2db,
    provide_context=True,
    dag=dag,
)


def print_fdr_info(**kwargs):
    ti = kwargs["ti"]
    fdr_info_df = ti.xcom_pull(task_ids="get_fdr_info", key="fdr_info_df")
    print(fdr_info_df)
    return None


_print_fdr_info = PythonOperator(
    task_id="print_fdr_info",
    python_callable=print_fdr_info,
    provide_context=True,
    dag=dag,
)


def fdr_info_2db(**kwargs):
    ti = kwargs["ti"]
    fdr_info_df = ti.xcom_pull(task_ids="get_fdr_info", key="fdr_info_df")
    fdr_info_df.to_sql(name="fdr_info_df", con=db_engine, if_exists="replace", index=False)
    return None


_fdr_info_2db = PythonOperator(
    task_id="fdr_info_2db",
    python_callable=fdr_info_2db,
    provide_context=True,
    dag=dag,
)

# task dag dependency 
_check_fdr_loader_obj >> _get_fdr_info
_get_fdr_info >> _get_fdr_ohlcv

# _get_fdr_ohlcv >> [_print_fdr_ohlcv, _print_fdr_info]
_get_fdr_ohlcv >> [_fdr_info_2db, _fdr_ohlcv_2db]
