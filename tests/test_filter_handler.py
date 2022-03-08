"""
   Copyright 2022 InfAI (CC SES)

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
"""

from ._util import *
import unittest
import ew_lib


class TestFilterHandlerBase:
    def test_ingestion_good_filters(self):
        filter_handler = self._init_filter_handler(filters=filters)
        self.assertIsNotNone(filter_handler.get_sources_timestamp())

    def test_ingestion_erroneous_filters(self):
        filter_handler = self._init_filter_handler(filters=filters_bad, timeout=True)
        self.assertIsNone(filter_handler.get_sources_timestamp())

    def test_filter_message_good_filters(self):
        filter_handler = self._init_filter_handler(filters=filters)
        self.assertIsNotNone(filter_handler.get_sources_timestamp())
        count = 0
        for source in data:
            for message in data[source]:
                try:
                    result = filter_handler.process_message(message=message, source=source)
                    self.assertIn(str(result), results)
                    count += 1
                except ew_lib.exceptions.NoFilterError:
                    pass
        self.assertEqual(count, len(results) - 1)

    def test_filter_message_erroneous_filters(self):
        filter_handler = self._init_filter_handler(filters=filters_bad, timeout=True)
        self.assertIsNone(filter_handler.get_sources_timestamp())
        count = 0
        for source in data:
            for message in data[source]:
                count += 1
                try:
                    filter_handler.process_message(message=message, source=source)
                except ew_lib.exceptions.NoFilterError:
                    count -= 1
        self.assertEqual(count, 0)

    def test_filter_bad_message(self):
        filter_handler = self._init_filter_handler(filters=filters)
        self.assertIsNotNone(filter_handler.get_sources_timestamp())
        count = 0
        for source in data_bad:
            for message in data_bad[source]:
                try:
                    filter_handler.process_message(message=message, source=source)
                    count += 1
                except ew_lib.exceptions.MessageIdentificationError:
                    pass
        self.assertEqual(count, 0)

    def test_get_export_args(self):
        filter_handler = self._init_filter_handler(filters=filters)
        self.assertIsNotNone(filter_handler.get_sources_timestamp())
        export_args = filter_handler.get_export_args(export_id="export-1")
        self.assertEqual(export_args["arg"], "test")


class TestFilterHandler(unittest.TestCase, TestFilterHandlerBase):
    def _init_filter_handler(self, filters, **kwargs):
        return test_filter_ingestion(test_obj=self, filters=filters)
