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

        # years = [2023]
        # months = [1]

        year_month_list = list(product(years, months))

        alerj_files = []

        for year, month in year_month_list:
            file = alerj_download_file(year, month)
            
            if file != "":
                alerj_files.append(file)
            # print(file)
            # os.remove(file)

        return alerj_files
    
    @task(task_id='pdf_to_parquet')
    def pdf_to_parquet(files_list):
        import shutil
        from include.alerj_modules.alerj_pdf_to_parquet import alerj_pdf_to_parquet
        from pathlib import Path

        for file in files_list:
            parquet_path = alerj_pdf_to_parquet(file)
            # shutil.move(parquet_path, r'/home/coutj/Downloads/test')
            shutil.rmtree(Path(parquet_path).parent)
            shutil.rmtree(Path(file).parent)
        


    download = download_files()
    to_parquet = pdf_to_parquet(download)
    fim = EmptyOperator(task_id='Fim')

    download >> to_parquet >> fim

     

alerj_salarios()