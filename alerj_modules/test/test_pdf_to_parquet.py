import pandas as pd
import os

from alerj_modules.alerj_pdf_to_parquet import alerj_pdf_to_parquet
from alerj_modules.alerj_download_file import alerj_download_file


def test_alerj_pdf_to_parquet():
    
    # Test file content

    pdf_path = alerj_download_file(2023, 1)
    
    parquet_path = alerj_pdf_to_parquet(pdf_path)

    col_names = ["nome", "cargo", "funcao",	"rendimento_funcionario","comissao",
                 "bonificacao","incorporado",
                 "trienio","abono_de_permanencia","ferias","redutor","ipalerj_mens.",
                 "pensao_alimenticia","previdencia","imposto_de_renda","indenizatoria",
                 "rendimento_liquido"]
    
    df_pdf = pd.read_parquet(parquet_path)


    assert set(col_names) == set(df_pdf.columns)

    os.remove(pdf_path)
    os.remove(parquet_path)