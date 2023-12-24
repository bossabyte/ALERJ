from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.hooks.base import BaseHook
from airflow.models import Variable


@dag(
    default_args={'owner': 'BossaByte'},
    dag_id='alerj_salarios',
    schedule=None,
    catchup=False,
    tags=['ALERJ', 'SALARIOS']
)
def alerj_salarios():

    @task
    def download_files():
        from include.alerj_modules.alerj_download_file import alerj_download_file
        from itertools import product
        import os

        years = list(range(2016, datetime.now().year))
        months = list(range(1, 13))

        years = [2023]
        months = [1]

        year_month_list = list(product(years, months))

        for year, month in year_month_list:
            file = alerj_download_file(year, month)
            print(file)

            os.remove(file)

    
    fim = EmptyOperator(task_id='Fim')

    download_files() >> fim

alerj_salarios()