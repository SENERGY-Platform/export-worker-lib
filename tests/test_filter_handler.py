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
import ew_lib._util.model
import json


with open("tests/resources/filter_message_results.json") as file:
    results: list = json.load(file)


class TestFilterHandler(unittest.TestCase):
    def __test_ingestion(self, filters):
        filter_handler = ew_lib.filter.FilterHandler()
        count = 0
        for filter in filters:
            try:
                if filter[ew_lib._util.model.FilterMessage.method] == ew_lib._util.model.Methods.put:
                    filter_handler.add(filter=filter[ew_lib._util.model.FilterMessage.payload])
                if filter[ew_lib._util.model.FilterMessage.method] == ew_lib._util.model.Methods.delete:
                    filter_handler.delete(
                        export_id=filter[ew_lib._util.model.FilterMessage.payload][ew_lib._util.model.FilterMessagePayload.export_id]
                    )
                count += 1
            except Exception:
                count += 1
        self.assertEqual(count, len(filters))
        for source in filter_handler.sources:
            self.assertIn(source, sources)
        return filter_handler

    def test_ingestion_good_filters(self):
        filter_handler = self.__test_ingestion(filters=filters)
        self.assertIsNotNone(filter_handler.sources_timestamp)

    def test_ingestion_erroneous_filters(self):
        filter_handler = self.__test_ingestion(filters=filters_bad)
        self.assertIsNone(filter_handler.sources_timestamp)

    def test_filter_message_good_filters(self):
        filter_handler = self.__test_ingestion(filters=filters)
        self.assertIsNotNone(filter_handler.sources_timestamp)
        count = 0
        for source in data:
            for message in data[source]:
                try:
                    result = filter_handler.filter_message(message=message, source=source)
                    self.assertIn(str(result), results)
                    count += 1
                except ew_lib.exceptions.NoFilterError:
                    pass
        self.assertEqual(count, len(results) - 1)

    def test_filter_message_erroneous_filters(self):
        filter_handler = self.__test_ingestion(filters=filters_bad)
        self.assertIsNone(filter_handler.sources_timestamp)
        count = 0
        for source in data:
            for message in data[source]:
                count += 1
                try:
                    filter_handler.filter_message(message=message, source=source)
                except ew_lib.exceptions.NoFilterError:
                    count -= 1
        self.assertEqual(count, 0)

    def test_filter_bad_message(self):
        filter_handler = self.__test_ingestion(filters=filters)
        self.assertIsNotNone(filter_handler.sources_timestamp)
        count = 0
        for source in data_bad:
            for message in data_bad[source]:
                try:
                    filter_handler.filter_message(message=message, source=source)
                    count += 1
                except ew_lib.exceptions.MessageIdentificationError:
                    pass
        self.assertEqual(count, 0)
