from datetime import datetime

from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
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


    @task(task_id='download_files')
    def download_files() -> list[tuple[str,str,str]]:
        from ALERJ.alerj_modules.alerj_download_file import alerj_download_file
        from itertools import product

        years = list(range(2016, datetime.now().year))
        months = list(range(1, 13))

        years = [2016]
        months = [1,2]

        year_month_list = list(product(years, months))

        alerj_files = []

        for year, month in year_month_list:
            file = alerj_download_file(year, month)
            
            if file != "":
                alerj_files.append((year, month, file))

        return alerj_files
    

    @task(task_id='pdf_to_parquet', max_active_tis_per_dag=3)
    def pdf_to_parquet(file):
        from shutil import rmtree
        from ALERJ.alerj_modules.alerj_pdf_to_parquet import alerj_pdf_to_parquet
        from pathlib import Path

        parquet_path = alerj_pdf_to_parquet(file[2], f"folha_{file[0]}_{file[1]}")
        rmtree(Path(file[2]).parent)

        return {
                "src": parquet_path, 
                "dst": f"raw/{file[0]}/"
        }


    @task(task_id='cleanup')
    def cleanup(files):
        from shutil import rmtree
        from pathlib import Path

        for file in files:
            rmtree(Path(file["src"]).parent)


    raw_files = download_files()
    parquet_files = pdf_to_parquet.expand(file=raw_files)

    upload_to_gcs = LocalFilesystemToGCSOperator.partial(
            task_id=f'upload_to_gcs',
            bucket="bossabyte",
            gcp_conn_id="gcp_conn",
            max_active_tis_per_dag=5
        ).expand_kwargs(parquet_files)
    
    
    gcs_raw_files = GCSListObjectsOperator(
        task_id='get_raw_files_list',
        bucket='bossabyte',
        prefix="raw/",
        gcp_conn_id="gcp_conn"
    )

    gcs_trusted_files = GCSListObjectsOperator(
        task_id='get_trusted_files',
        bucket='bossabyte',
        prefix="trusted/",
        gcp_conn_id="gcp_conn"
    )

    @task(task_id='files_to_transform')
    def files_to_transform(raw_list, trusted_list):
        from pathlib import Path
        print(raw_list)

        raw_file_names = [f"{Path(file).parent.name}/{Path(file).name}" for file in raw_list]
        trusted_file_names = [f"{Path(file).parents[1].name}/{Path(file).parent.name}" for file in trusted_list]

        trusted_file_names = list(dict.fromkeys(trusted_file_names))
        print(trusted_file_names)

        transform_files = list(set(raw_file_names) - set(trusted_file_names))

        transform_dir = [{'notebook_params': {'file_name': file }} for file in transform_files]

        return transform_dir

    
    transform_list = files_to_transform(gcs_raw_files.output, gcs_trusted_files.output)

    run_databricks = DatabricksRunNowOperator.partial(
            task_id="Transform",
            databricks_conn_id="databricks",
            job_id=Variable.get('databricks_jobid'),
            max_active_tis_per_dagrun=2).expand_kwargs(transform_list)


    clean = cleanup(parquet_files)

    fim = EmptyOperator(task_id='end')

    (raw_files >> parquet_files >> upload_to_gcs >> clean >> gcs_raw_files >> 
     gcs_trusted_files >> transform_list >> run_databricks >> fim)

    
alerj_salarios()
