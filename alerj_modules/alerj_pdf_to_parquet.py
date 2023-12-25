from re import sub
from pathlib import Path
import tabula
import pandas as pd
from unidecode import unidecode
from tempfile import mkdtemp


def column_name_cleanup(col_name: str):

    return unidecode(
                    sub(r'[^\w.]', "_", col_name)
                    .replace('.', '').lower()
            )


def number_cleanup(number):
    return number.replace(".", "").replace(",", ".")


def alerj_pdf_to_parquet(file_path: str) -> str:

    print('pdf file:', file_path)

    pdf = tabula.read_pdf(
        file_path,
        # force_subprocess=True,
        lattice=True,
        pandas_options={'header': None},
        pages='all')

    # check last page/df
    words_to_drop = ['TOTAIS']
    pdf[-1] = pdf[-1].drop(pdf[-1][pdf[-1].iloc[:, 2].str.contains('|'.join(words_to_drop), na=False)].index.to_list())
    pdf[-2] = pdf[-2].drop(pdf[-2][pdf[-2].iloc[:, 2].str.contains('|'.join(words_to_drop), na=False)].index.to_list())

    df_complete = pd.concat(pdf)
    
    df_complete.dropna(axis=1, how='all', inplace=True)
    df_complete.dropna(thresh=3, inplace=True)

    columns = df_complete.iloc[0,:].to_list()
    columns = list(map(column_name_cleanup, columns))
    df_complete.columns = columns
    df_complete.columns.values[5] = 'bonificacao'

    df_complete.drop(index=0, inplace=True) # drop first row with col names

    temp_dir = mkdtemp(prefix='alerj_parquet_')

    file_destin = f"{Path(temp_dir, Path(file_path).stem)}.parquet"
    print('Parquet:', file_destin)
 
    df_complete.to_parquet(file_destin, index=False)
    # df_complete.to_csv('test.csv', index=False)

    return file_destin


if __name__ == '__main__':
    # alerj_pdf_to_parquet('/tmp/Alerj_2016_2_gm_523dm/folha-de-pagamento-2016-02.pdf')
    alerj_pdf_to_parquet('/tmp/Alerj_2016_1_tshl8fq1/folha-de-pagamento-2016-01.pdf')
    # alerj_pdf_to_parquet('/home/coutj/Downloads/folha-de-pagamento-2023-06_v.1.pdf')

