from pathlib import Path

import requests
from lxml import html
from tempfile import mkdtemp


DOMINIO = 'https://transparencia.alerj.rj.gov.br'
URL_ALERJ_TRANSPARENCIA = 'https://transparencia.alerj.rj.gov.br/section/report/73'


def alerj_download_file(year: int, month: int) -> str:

    dict_month = {
        1: 'janeiro', 2: 'fevereiro', 3: 'março',
        4: 'abril', 5: 'maio', 6: 'junho',
        7: 'julho', 8: 'agosto', 9: 'setembro',
        10: 'outubro', 11: 'novembro', 12: 'dezembro'
    }

    month_text = dict_month.get(month).upper()

    r = requests.get(URL_ALERJ_TRANSPARENCIA)
    tree = html.fromstring(r.content)

    link = tree.xpath(f"//div[@id='collapse-{year}']//div[contains(text(), '{month_text}')]/following-sibling::div/a")

    if len(link) == 0:
        return ""
    else:
        link = link[0] 

    file_resp = requests.get(f'{DOMINIO}{link.get("href")}', allow_redirects=True)

    if file_resp.content == "" or file_resp.status_code != 200:
        return ""

    temp_dir = mkdtemp(prefix=f'Alerj_{year}_{month}_',)
    
    file_name = Path(file_resp.url).name
    file_path = Path(temp_dir, file_name)

    with open(file_path, mode='wb') as file:
        file.write(file_resp.content)

    print(file_path)

    return file_path.as_posix()


if __name__ == '__main__':

    alerj_download_file(2016, 3)
