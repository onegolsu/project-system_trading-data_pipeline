from datetime import datetime

import pandas as pd

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from system_trading.controller.db import get_db_engine
from system_trading.api_loader.dart import get_dart_api_loader, get_dart_fss_loader

DART_API_CFG = {
    "year": 2023,
    "reprt_code": 11014,
    # (11011 : 연간보고서) / (11012 : 반기보고서) / (11013 : 1분기보고서) / (11014 : 3분기보고서)
}

db_engine = get_db_engine()

default_args = {
    "owner": "system_trading",
    "depends_on_past": False,
    "start_date": datetime(2023, 1, 1),
}

# DAG 인스턴스 생성
dag = DAG(
    "system_trading_dart_v1",
    default_args=default_args,
    schedule_interval="0 0 * * 1#1",  # 매월 첫번째 월요일
    catchup=False,
)


def check_dart_fss_loader_obj(**kwargs):
    dart_fss_loader = get_dart_fss_loader()
    return None


_check_dart_fss_loader_obj = PythonOperator(
    task_id="check_dart_fss_loader_obj",
    python_callable=check_dart_fss_loader_obj,
    provide_context=True,
    dag=dag,
)


def check_dart_api_loader_obj(**kwargs):
    dart_api_loader = get_dart_api_loader()
    return None


_check_dart_api_loader_obj = PythonOperator(
    task_id="check_dart_api_loader_obj",
    python_callable=check_dart_api_loader_obj,
    provide_context=True,
    dag=dag,
)


def get_dart_fss_info(**kwargs):
    dart_fss_loader = get_dart_fss_loader()
    dart_info_df = dart_fss_loader.get_info_df()
    ti = kwargs["ti"]
    ti.xcom_push(key="dart_info_df", value=dart_info_df)
    return None


_get_dart_fss_info = PythonOperator(
    task_id="get_dart_fss_info",
    python_callable=get_dart_fss_info,
    provide_context=True,
    dag=dag,
)


def filter_dart_fss_info(**kwargs):
    ti = kwargs["ti"]
    dart_info_df = ti.xcom_pull(task_ids="get_dart_fss_info", key="dart_info_df")
    filtered_dart_info_df = dart_info_df[~(dart_info_df["stock_code"].isna())]
    corp_codes = sorted(set(filtered_dart_info_df["corp_code"]))
    ti.xcom_push(key="corp_codes", value=corp_codes)
    ti.xcom_push(key="filtered_dart_info_df", value=filtered_dart_info_df)
    return None


_filter_dart_fss_info = PythonOperator(
    task_id="filter_dart_fss_info",
    python_callable=filter_dart_fss_info,
    provide_context=True,
    dag=dag,
)


def get_dart_api_fundamental(**kwargs):
    ti = kwargs["ti"]
    dart_api_loader = get_dart_api_loader()
    corp_codes = ti.xcom_pull(task_ids="filter_dart_fss_info", key="corp_codes")
    dart_api_fundamental_df = dart_api_loader.get_corps_fundamental_df(
        corp_codes=corp_codes, year=DART_API_CFG["year"], reprt_code=DART_API_CFG["reprt_code"]
    )
    ti.xcom_push(key="dart_api_fundamental_df", value=dart_api_fundamental_df)
    return None


_get_dart_api_fundamental = PythonOperator(
    task_id="get_dart_api_fundamental",
    python_callable=get_dart_api_fundamental,
    provide_context=True,
    dag=dag,
)


def print_dart_fss_info(**kwargs):
    ti = kwargs["ti"]
    filtered_dart_info_df = ti.xcom_pull(task_ids="filter_dart_fss_info", key="filtered_dart_info_df")
    print(filtered_dart_info_df)
    return None


_print_dart_fss_info = PythonOperator(
    task_id="print_dart_fss_info",
    python_callable=print_dart_fss_info,
    provide_context=True,
    dag=dag,
)


def dart_fss_info_2db(**kwargs):
    ti = kwargs["ti"]
    filtered_dart_info_df = ti.xcom_pull(task_ids="filter_dart_fss_info", key="filtered_dart_info_df")
    filtered_dart_info_df.to_sql(name="dart_info_df", con=db_engine, if_exists="replace", index=False)
    return None


_dart_fss_info_2db = PythonOperator(
    task_id="dart_fss_info_2db",
    python_callable=dart_fss_info_2db,
    provide_context=True,
    dag=dag,
)


def print_dart_api_fundamental(**kwargs):
    ti = kwargs["ti"]
    dart_api_fundamental_df = ti.xcom_pull(task_ids="get_dart_api_fundamental", key="dart_api_fundamental_df")
    print(dart_api_fundamental_df)
    return None


_print_dart_api_fundamental = PythonOperator(
    task_id="print_dart_api_fundamental",
    python_callable=print_dart_api_fundamental,
    provide_context=True,
    dag=dag,
)


def dart_api_fundamental_2db(**kwargs):
    ti = kwargs["ti"]
    dart_api_fundamental_df = ti.xcom_pull(task_ids="get_dart_api_fundamental", key="dart_api_fundamental_df")
    dart_api_fundamental_df.to_sql(name="dart_fundamental_df", con=db_engine, if_exists="replace", index=False)
    return None


_dart_api_fundamental_2db = PythonOperator(
    task_id="dart_api_fundamental_2db",
    python_callable=dart_api_fundamental_2db,
    provide_context=True,
    dag=dag,
)


# task dag dependency
[_check_dart_api_loader_obj, _check_dart_fss_loader_obj] >> _get_dart_fss_info
_get_dart_fss_info >> _filter_dart_fss_info
_filter_dart_fss_info >> _get_dart_api_fundamental

_get_dart_api_fundamental >> [_dart_api_fundamental_2db, _dart_fss_info_2db]
# _get_dart_api_fundamental >> [_print_dart_fss_info, _print_dart_api_fundamental]
