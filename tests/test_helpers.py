#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
#  tests/test_helpers.py
#
#  Copyleft 2018 VIAA vzw
#  <admin@viaa.be>
#
#  @author: https://github.com/maartends
#
#######################################################################
#
#  tests/test_helpers.py
#
#######################################################################

import json
import unittest
from meemoo.helpers import get_from_event
from tests.resources import S3_MOCK_EVENT

class TestHelperFunctions(unittest.TestCase):

    def test_get_bucket_from_event(self):
        bucket = get_from_event(json.loads(S3_MOCK_EVENT), 'bucket')
        self.assertEqual(bucket, 'MAM_HighresVideo')

    def test_get_object_key_from_event(self):
        bucket = get_from_event(json.loads(S3_MOCK_EVENT), 'object_key')
        self.assertEqual(bucket, '191213-VAN___statement_De_ideale_wereld___Don_12_December_2019-1983-d5be522e-3609-417a-a1f4-5922854620c8.MXF')

    def test_get_md5_from_event(self):
        bucket = get_from_event(json.loads(S3_MOCK_EVENT), 'md5')
        self.assertEqual(bucket, '7ef01fd710fec9a175d28c4a31dc49a2')

if __name__ == '__main__':
        unittest.main()


# vim: tabstop=8 expandtab shiftwidth=4 softtabstop=4 smartindent
