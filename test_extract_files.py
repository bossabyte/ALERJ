import os
import hashlib

# import pytest
from extract_files import alerj_download_file
from pathlib import Path


def test_alerj_download_file():

    # test file download

    expected_file_name = 'folha-de-pagamento-2023-01.pdf'
    file = alerj_download_file(2023, 1)

    assert Path(file).name == expected_file_name
    os.remove(file)

    # test file content

    expected_hash_2023_06 = '53dcdddbcc793bf9f6361e68263255a0'

    file = alerj_download_file(2023, 6)

    file_hash = hashlib.md5(open(file, 'rb').read()).hexdigest()

    assert file_hash == expected_hash_2023_06

    os.remove(file)

