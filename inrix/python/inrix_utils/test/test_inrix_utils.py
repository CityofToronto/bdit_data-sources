import unittest
import argparse
import sys
from io import StringIO
from argparse import ArgumentError
from inrix_util import _validate_yyyymm_range, _validate_yearsjson, parse_args, _validate_multiple_yyyymm_range
from contextlib import contextmanager


@contextmanager
def capture_sys_output():
    capture_out, capture_err = StringIO(), StringIO()
    current_out, current_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = capture_out, capture_err
        yield capture_out, capture_err
    finally:
        sys.stdout, sys.stderr = current_out, current_err

class CreateUtilsTestCase(unittest.TestCase):
    '''Tests for `inrix_util.py`'''

    def test_valid_yyyymm_range(self):
        '''Test if the range ['201206','201403'] produces the right range'''
        valid_result = {2012:range(6,13),
                        2013:range(1,13),
                        2014:range(1,4)}
        self.assertEqual(valid_result, 
                         _validate_yyyymm_range(['201206', '201403']))

    def test_valid_yyyymm_sameyear_range(self):
        '''Test if the range ['201604','201606'] produces the right range'''
        valid_result = {2016:range(4,7)}
        self.assertEqual(valid_result, 
                         _validate_yyyymm_range(['201604', '201606']))   
        
    def test_outoforder_yyyymm_range(self):
        '''Test if the proper error is thrown with ['201403', '201206']'''
        with self.assertRaises(ValueError) as cm:
            _validate_yyyymm_range(['201403', '201206'])
        self.assertEqual('Start date 201403 after end date 201206', str(cm.exception))
        
    def test_outoforder_yyyymm_range_sameyear(self):
        '''Test if the proper error is thrown with ['201606', '201603']'''
        with self.assertRaises(ValueError) as cm:
            _validate_yyyymm_range(['201606', '201603'])
        self.assertEqual('Start date 201606 after end date 201603', str(cm.exception))

    def test_invalid_yyyymm_value(self):
        '''Test if the proper error is thrown with an invalid YYYYMM ['201206','201217']'''
        with self.assertRaises(ValueError) as cm:
            _validate_yyyymm_range(['201206', '201217'])
        self.assertEqual('201217 is not a valid year-month value of format YYYYMM', str(cm.exception))
        
    def test_multiple_yyyymm_range(self):
        '''Test if using an overlapping range produces the right result'''
        test_range = [['201203', '201301'],['201207', '201209']]
        valid_result = {2012:set(range(3,13)),
                        2013:range(1,2)}
        self.assertEqual(valid_result,_validate_multiple_yyyymm_range(test_range))

    def test_multiple_yyyymm_range_distinct(self):
        '''Test if using distinct ranges in the same year produces the right result'''
        test_range = [['201203', '201207'],['201209', '201303']]
        valid_result = {2012:set.union(set(range(3,8)), set(range(9,13))),
                        2013:range(1,4)}
        self.assertEqual(valid_result,_validate_multiple_yyyymm_range(test_range))
        
    def test_multiple_yyyymm_range_single(self):
        '''Test if a single range produces the right result'''
        test_range = [['201203', '201301']]
        valid_result = {2012:range(3,13),
                        2013:range(1,2)}
        self.assertEqual(valid_result,_validate_multiple_yyyymm_range(test_range))
    
    def test_valid_yearsjson(self):
        '''Test if a correct json produces the right output'''
        valid_result = {2012:range(6,13),
                        2013:range(1,13),
                        2014:range(1,4)}
        test_input = {2012:[6,12],
                      2013:[1,12],
                      2014:[1,3]}
        self.assertEqual(valid_result, 
                         _validate_yearsjson(test_input))

    def test_years_yearsjson(self):
        '''Test if an incorrect year produces correct error message'''
        incorrect_year = {1999:[6,12],
                          2013:[1,12],
                          2014:[1,3]}
        with self.assertRaises(ValueError) as cm:
            _validate_yearsjson(incorrect_year)
        self.assertEqual('Year 1999 is invalid input', str(cm.exception))

    def test_month_yearsjson(self):
        '''Test if an incorrect month produces correct error message'''
        incorrect_month = {2012:[6,13],
                          2013:[1,12],
                          2014:[1,3]}
        with self.assertRaises(ValueError) as cm:
            _validate_yearsjson(incorrect_month)
        self.assertEqual('For year 2012, month 13 is not a valid month', str(cm.exception))

    def test_month_ooo_yearsjson(self):
        '''Test if months out of order produces correct error message'''
        incorrect_month_range = {2012:[12,2],
                          2013:[1,12],
                          2014:[1,3]}
        with self.assertRaises(ValueError) as cm:
            _validate_yearsjson(incorrect_month_range)
        self.assertEqual('For year 2012, first month 12 comes after second month 2', str(cm.exception))
        
class ArgParseTestCase(unittest.TestCase):
    def __init__(self, *args, **kwargs):
        self.testing_params = {'prog':'TESTING', 'usage':''}
        super(ArgParseTestCase, self).__init__(*args, **kwargs)

    def test_years_y_single(self):
        '''Test if a single pair of years produces the right values'''
        valid_result = [['201407','201506']]
        args = parse_args('-i -y 201407 201506'.split())
        self.assertEqual(valid_result, args.years)
        
    def test_years_y_multiple(self):
        '''Test if a single pair of years produces the right values'''
        valid_result = [['201203', '201301'],['201207', '201209']]
        args = parse_args('-i -y 201203 201301 -y 201207 201209'.split())
        self.assertEqual(valid_result, args.years)
        
    def test_years_y_only_one(self):
        '''Test if a single pair of years produces the right values'''
        with self.assertRaises(SystemExit) as cm, capture_sys_output() as (stdout, stderr):
            args = parse_args('-i -y 201207'.split(), **self.testing_params)
        self.assertEqual(2, cm.exception.code)
        self.assertEqual('usage: \nTESTING: error: argument -y/--years: expected 2 arguments\n', stderr.getvalue())
        
    
if __name__ == '__main__':
    unittest.main()
