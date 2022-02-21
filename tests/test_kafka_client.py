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
import json
import time


with open("tests/resources/exports_results.json") as file:
    results: list = json.load(file)

with open("tests/resources/exports_batch_results.json") as file:
    batch_results: list = json.load(file)


class TestKafkaClient(unittest.TestCase):
    def __init_client(self, filters_path):
        test_kafka_consumer = TestKafkaConsumer()
        filter_handler = ew_lib.filter.FilterHandler(filter_consumer=TestFilterConsumer(path=filters_path))
        kafka_client = ew_lib.KafkaClient(
            kafka_consumer=test_kafka_consumer,
            filter_handler=filter_handler
        )
        filter_handler.start()
        kafka_client.start()
        time.sleep(0.1)
        return kafka_client, filter_handler, test_kafka_consumer

    def __close(self, kafka_client, filter_handler):
        kafka_client.stop()
        filter_handler.stop()

    def test_get_exports_good_filters(self):
        kafka_client, filter_handler, test_kafka_consumer = self.__init_client(filters_path="tests/resources/filters.json")
        e_count = 0
        m_count = 0
        while not test_kafka_consumer.empty():
            exports = kafka_client.get_exports(timeout=1.0)
            if exports:
                self.assertIn(exports, results)
                e_count += 1
            m_count += 1
        self.assertEqual(e_count, len(results))
        self.assertEqual(m_count, len(data))
        self.__close(kafka_client=kafka_client, filter_handler=filter_handler)

    def test_get_exports_batch_good_filters(self):
        kafka_client, filter_handler, test_kafka_consumer = self.__init_client(filters_path="tests/resources/filters.json")
        e_count = 0
        m_count = 0
        while not test_kafka_consumer.empty():
            exports_batch = kafka_client.get_exports_batch(timeout=5.0, limit=2)
            if exports_batch:
                self.assertIn(exports_batch, batch_results)
                e_count += 1
            m_count += 1
        self.assertEqual(e_count, len(batch_results))
        self.assertEqual(m_count, len(data) / 2)
        self.__close(kafka_client=kafka_client, filter_handler=filter_handler)

    def test_get_exports_erroneous_filters(self):
        kafka_client, filter_handler, test_kafka_consumer = self.__init_client(filters_path="tests/resources/filters_bad.json")
        count = 0
        while not test_kafka_consumer.empty():
            self.assertIsNone(kafka_client.get_exports(timeout=1.0))
            count += 1
        self.assertEqual(count, len(data))
        self.__close(kafka_client=kafka_client, filter_handler=filter_handler)

    def test_get_exports_batch_erroneous_filters(self):
        kafka_client, filter_handler, test_kafka_consumer = self.__init_client(filters_path="tests/resources/filters_bad.json")
        count = 0
        while not test_kafka_consumer.empty():
            self.assertIsNone(kafka_client.get_exports_batch(timeout=5.0, limit=2))
            count += 1
        self.assertEqual(count, len(data) / 2)
        self.__close(kafka_client=kafka_client, filter_handler=filter_handler)
