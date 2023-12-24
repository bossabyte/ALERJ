import tabula
import pandas as pd
from unidecode import unidecode
import numpy as np


def column_name_cleanup(col_name: str):
    return unidecode(col_name.lower().strip()
                     .replace('\r', " ").replace("  ", " ")
                     .replace(" ", "_"))


def number_cleanup(number):
    print(number)
    return number.replace(".", "").replace(",", ".")


def alerj_pdf_to_text(file_path: str):

    pdf = tabula.read_pdf(file_path, pages='all')

    print(len(pdf))

    df_complete = pd.concat(pdf)


    df_complete = df_complete.query("NOME.str.len() < 120")
   

    df_complete.columns = map(column_name_cleanup, df_complete.columns)
    print(df_complete)


if __name__ == '__main__':
    alerj_pdf_to_text('/home/coutj/Downloads/folha-de-pagamento-2023-06_v.1.pdf')

