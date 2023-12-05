import datetime as dt
from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from system_trading.controller.db import get_db_engine
from system_trading.api_loader.pykrx import get_pykrx_loader

## pykrx_cfg
PYKRX_CFG = {
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
    "system_trading_pykrx",
    default_args=default_args,
    schedule_interval="0 0 * * 1#1",  # 매월 첫번째 월요일
    catchup=False,
)


def check_pykrx_loader_obj(**kwargs):
    pykrx_loader = get_pykrx_loader()
    return None


_check_pykrx_loader_obj = PythonOperator(
    task_id="check_pykrx_loader_obj",
    python_callable=check_pykrx_loader_obj,
    provide_context=True,
    dag=dag,
)


def get_kospi_trader(**kwargs):
    pykrx_loader = get_pykrx_loader()
    KOSPI_trader_df = pykrx_loader.get_stock_trader_df(
        StockCode="KOSPI", start=PYKRX_CFG["start"], end=PYKRX_CFG["end"]
    )
    KOSPI_trader_df["Market"] = "KOSPI"
    ti = kwargs["ti"]
    ti.xcom_push(key="KOSPI_trader_df", value=KOSPI_trader_df)
    return None


_get_kospi_trader = PythonOperator(
    task_id="get_kospi_trader",
    python_callable=get_kospi_trader,
    provide_context=True,
    dag=dag,
)


def get_kosdaq_trader(**kwargs):
    pykrx_loader = get_pykrx_loader()
    KOSDAQ_trader_df = pykrx_loader.get_stock_trader_df(
        StockCode="KOSDAQ", start=PYKRX_CFG["start"], end=PYKRX_CFG["end"]
    )
    KOSDAQ_trader_df["Market"] = "KOSDAQ"
    ti = kwargs["ti"]
    ti.xcom_push(key="KOSDAQ_trader_df", value=KOSDAQ_trader_df)
    return None


_get_kosdaq_trader = PythonOperator(
    task_id="get_kosdaq_trader",
    python_callable=get_kosdaq_trader,
    provide_context=True,
    dag=dag,
)


def concat_trader(**kwargs):
    ti = kwargs["ti"]
    KOSPI_trader_df = ti.xcom_pull(task_ids="get_kospi_trader", key="KOSPI_trader_df")
    KOSDAQ_trader_df = ti.xcom_pull(task_ids="get_kosdaq_trader", key="KOSDAQ_trader_df")
    trader_df = pd.concat([KOSDAQ_trader_df, KOSPI_trader_df], axis=0)
    ti.xcom_push(key="trader_df", value=trader_df)
    return None


_concat_trader = PythonOperator(
    task_id="concat_trader",
    python_callable=concat_trader,
    provide_context=True,
    dag=dag,
)


def print_trader(**kwargs):
    ti = kwargs["ti"]
    trader_df = ti.xcom_pull(task_ids="concat_trader", key="trader_df")
    print(trader_df)

    return None


_print_trader = PythonOperator(
    task_id="print_trader",
    python_callable=print_trader,
    provide_context=True,
    dag=dag,
)


def trader_2db(**kwargs):
    ti = kwargs["ti"]
    trader_df = ti.xcom_pull(task_ids="concat_trader", key="trader_df")
    trader_df.to_sql(name="trader_df", con=db_engine, if_exists="replace", index=False)
    return None


_trader_2db = PythonOperator(
    task_id="trader_2db",
    python_callable=trader_2db,
    provide_context=True,
    dag=dag,
)

# task dag dependency
_check_pykrx_loader_obj >> [_get_kospi_trader, _get_kosdaq_trader]
[_get_kospi_trader, _get_kosdaq_trader] >> _concat_trader
_concat_trader >> _trader_2db
