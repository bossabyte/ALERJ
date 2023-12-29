from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
#from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
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
        from ALERJ.alerj_modules.alerj_download_file import alerj_download_file
        from itertools import product
        import os

        years = list(range(2016, datetime.now().year))
        months = list(range(1, 13))

        years = [2023]
        months = [1]

        year_month_list = list(product(years, months))

        alerj_files = []

        for year, month in year_month_list:
            file = alerj_download_file(year, month)
            
            if file != "":
                alerj_files.append(file)
            # print(file)
            # os.remove(file)

        return alerj_files
    
    @task(task_id='pdf_to_parquet', max_active_tis_per_dag=4)
    def pdf_to_parquet(file):
        import shutil
        from ALERJ.alerj_modules.alerj_pdf_to_parquet import alerj_pdf_to_parquet
        from pathlib import Path

        parquet_path = alerj_pdf_to_parquet(file)
        shutil.rmtree(Path(file).parent)

        return parquet_path

    @task 
    def printnanana(a):
        print(a)
    
    to_databricks = DatabricksRunNowOperator(
            task_id = 'to_databricks',
            databricks_conn_id = 'databricks_default',
            job_id = "866773471737951"
        )

    download = download_files()
    to_parquet = pdf_to_parquet.expand(file=download)
    task_nanana = printnanana(to_parquet)
    fim = EmptyOperator(task_id='Fim')


    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id=f'upload_to_gcs',
        src=to_parquet,
        dst="bossabyte/raw",
        bucket="bossabyte",
        gcp_conn_id="gcp_conn",
    )

    download >> to_parquet >> to_databricks >> task_nanana >> fim

    
alerj_salarios()
