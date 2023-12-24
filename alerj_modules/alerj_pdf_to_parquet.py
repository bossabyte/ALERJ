from pathlib import Path
import tabula
import pandas as pd
from unidecode import unidecode
from tempfile import mkdtemp


def column_name_cleanup(col_name: str):
    return unidecode(col_name.lower().strip()
                     .replace('\r', " ").replace("  ", " ")
                     .replace(" ", "_"))


def number_cleanup(number):
    return number.replace(".", "").replace(",", ".")


def alerj_pdf_to_parquet(file_path: str) -> str:

    pdf = tabula.read_pdf(
        file_path,
        force_subprocess=True,
        pages='all')

    df_complete = pd.concat(pdf)   

    df_complete.columns = map(column_name_cleanup, df_complete.columns)
    df_complete.columns.values[5] = 'bonificacao'

    temp_dir = mkdtemp(suffix='alerj_parquet_')

    file_destin = f"{Path(temp_dir, Path(file_path).stem)}.parquet"

    df_complete.to_parquet(file_destin, index=False)
    # df_complete.to_csv('test.csv', index=False)

    return file_destin


if __name__ == '__main__':
    alerj_pdf_to_parquet('/home/coutj/Downloads/folha-de-pagamento-2023-06_v.1.pdf')

