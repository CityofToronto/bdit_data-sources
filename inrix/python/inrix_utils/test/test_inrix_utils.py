import unittest
from create_index import _validate_yyyymm_range, _validate_yearsjson

class CreateIndexTestCase(unittest.TestCase):
    '''Tests for `create_index.py`'''

    def test_valid_yyyymm_range(self):