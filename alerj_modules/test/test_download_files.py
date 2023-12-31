import os
import hashlib

# import pytest
from alerj_modules.alerj_download_file import alerj_download_file
from pathlib import Path


def test_alerj_download_file():

    # Test file download

    expected_file_name = 'folha-de-pagamento-2023-01.pdf'
    file = alerj_download_file(2023, 1)

    assert Path(file).name == expected_file_name
    os.remove(file)

    # Test file content

    expected_hash_2023_06 = '53dcdddbcc793bf9f6361e68263255a0'

    file = alerj_download_file(2023, 6)

    file_hash = hashlib.md5(open(file, 'rb').read()).hexdigest()

    assert file_hash == expected_hash_2023_06

    os.remove(file)

    # Test file not found

    file = alerj_download_file(2000, 1)

    assert file == ""

