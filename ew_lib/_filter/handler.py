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

__all__ = ("FilterHandler",)

from .._util import hash_dict, get_value, logger, model
from .. import exceptions
from .. import builders
from .consumer import FilterConsumer
import typing
import threading
import time

# Filter tree
#             1st lvl         2nd lvl         3rd lvl           4th lvl
# devices     service_id      device_id       mapping hash      [export_id]
# analytics   pipeline_id     operator_id     mapping hash      [export_id]


def hash_mapping(mapping: typing.Dict):
    try:
        return hash_dict(mapping)
    except Exception as ex:
        raise exceptions.HashMappingError(f"{ex} - {mapping}")


def parse_mapping(mapping: typing.Dict) -> typing.List[typing.Dict]:
    try:
        parsed_mapping = list()
        for key, value in mapping.items():
            dst_path, val_type = key.split(":")
            parsed_mapping.append(
                {
                    model.Mapping.src_path: value,
                    model.Mapping.dst_path: dst_path,
                    model.Mapping.type: val_type
                }
            )
        return parsed_mapping
    except Exception as ex:
        raise exceptions.ParseMappingError(f"{ex} - {mapping}")


def mapper(mappings: typing.List, msg: typing.Dict) -> typing.Generator:
    for mapping in mappings:
        src_path = mapping[model.Mapping.src_path].split(".")
        yield mapping[model.Mapping.dst_path], get_value(src_path, msg, len(src_path) - 1)


class FilterHandler:
    def __init__(self, filter_consumer: FilterConsumer, fallback_delay: int = 1):
        self.__filter_consumer = filter_consumer
        self.__fallback_delay = fallback_delay
        self.__thread = threading.Thread(name=f"{self.__class__.__name__}-{uuid.uuid4()}", target=self.__handle_filter, daemon=True)
        self.__lock = threading.Lock()
        self.__msg_identifier_keys = set()
        self.__msg_identifiers = dict()
        self.__msg_filters = dict()
        self.__mappings = dict()
        self.__sources = set()
        self.__exports = dict()
        self.__mapping_export_map = dict()
        self.__msg_identifiers_export_map = dict()
        self.__sources_export_map = dict()
        self.__updated = None
        self.__stop = False

    def __add_filter(self, lvl_one_value, lvl_two_value, m_hash, export_id):
        try:
            try:
                self.__msg_filters[lvl_one_value][lvl_two_value][m_hash].add(export_id)
            except KeyError:
                if lvl_one_value not in self.__msg_filters:
                    self.__msg_filters[lvl_one_value] = dict()
                if lvl_two_value not in self.__msg_filters[lvl_one_value]:
                    self.__msg_filters[lvl_one_value][lvl_two_value] = dict()
                if m_hash not in self.__msg_filters[lvl_one_value][lvl_two_value]:
                    self.__msg_filters[lvl_one_value][lvl_two_value][m_hash] = {export_id}
        except Exception as ex:
            raise exceptions.AddFilterError(ex)

    def __del_filter(self, lvl_one_value, lvl_two_value, m_hash, export_id):
        try:
            self.__msg_filters[lvl_one_value][lvl_two_value][m_hash].discard(export_id)
            if not self.__msg_filters[lvl_one_value][lvl_two_value][m_hash]:
                del self.__msg_filters[lvl_one_value][lvl_two_value][m_hash]
                if not self.__msg_filters[lvl_one_value][lvl_two_value]:
                    del self.__msg_filters[lvl_one_value][lvl_two_value]
                    if not self.__msg_filters[lvl_one_value]:
                        del self.__msg_filters[lvl_one_value]
        except Exception as ex:
            raise exceptions.DeleteFilterError(ex)

    def __add_mapping(self, mapping: typing.Dict, m_hash: str, export_id: str):
        try:
            if m_hash not in self.__mappings:
                self.__mappings[m_hash] = parse_mapping(mapping=mapping)
            if m_hash not in self.__mapping_export_map:
                self.__mapping_export_map[m_hash] = {export_id}
            else:
                self.__mapping_export_map[m_hash].add(export_id)
        except Exception as ex:
            raise exceptions.AddMappingError(ex)

    def __del_mapping(self, m_hash: str, export_id: str):
        try:
            self.__mapping_export_map[m_hash].discard(export_id)
            if not self.__mapping_export_map[m_hash]:
                del self.__mappings[m_hash]
                del self.__mapping_export_map[m_hash]
        except Exception as ex:
            raise exceptions.DeleteMappingError(ex)

    def __add_msg_identifier(self, lvl_one_key: str, lvl_two_key: str, export_id: str):
        try:
            if lvl_one_key not in self.__msg_identifiers:
                self.__msg_identifiers[lvl_one_key] = lvl_two_key
                self.__msg_identifier_keys.add(lvl_one_key)
            else:
                if self.__msg_identifiers[lvl_one_key] != lvl_two_key:
                    raise exceptions.MessageIdentifierMissmatchError((self.__msg_identifiers[lvl_one_key], lvl_two_key))
            if lvl_one_key not in self.__msg_identifiers_export_map:
                self.__msg_identifiers_export_map[lvl_one_key] = {export_id}
            else:
                self.__msg_identifiers_export_map[lvl_one_key].add(export_id)
        except Exception as ex:
            raise exceptions.AddMessageIdentifierError(ex)

    def __del_msg_identifier(self, lvl_one_key: str, export_id: str):
        try:
            self.__msg_identifiers_export_map[lvl_one_key].discard(export_id)
            if not self.__msg_identifiers_export_map[lvl_one_key]:
                del self.__msg_identifiers[lvl_one_key]
                self.__msg_identifier_keys.discard(lvl_one_key)
                del self.__msg_identifiers_export_map[lvl_one_key]
        except Exception as ex:
            raise exceptions.DeleteMessageIdentifierError(ex)

    def __add_source(self, source: str, export_id: str):
        try:
            self.__sources.add(source)
            if source not in self.__sources_export_map:
                self.__sources_export_map[source] = {export_id}
            else:
                self.__sources_export_map[source].add(export_id)
        except Exception as ex:
            raise exceptions.AddSourceError(ex)

    def __del_source(self, source: str, export_id: str):
        try:
            self.__sources_export_map[source].discard(export_id)
            if not self.__sources_export_map[source]:
                self.__sources.discard(source)
                del self.__sources_export_map[source]
        except Exception as ex:
            raise exceptions.DeleteSourceError(ex)

    def __add_export(self, export_id: str, source: str, lvl_one_key: str, lvl_one_value: str, lvl_two_value: str, m_hash: str):
        try:
            self.__exports[export_id] = {
                model.Filter.source: source,
                model.Filter.lvl_one_key: lvl_one_key,
                model.Filter.lvl_one_value: lvl_one_value,
                model.Filter.lvl_two_value: lvl_two_value,
                model.Filter.m_hash: m_hash
            }
        except Exception as ex:
            raise exceptions.AddExportError(ex)

    def __del_export(self, export_id: str):
        try:
            del self.__exports[export_id]
        except Exception as ex:
            raise exceptions.DeleteExportError(ex)

    def __add(self, source: str, lvl_one_key: str, lvl_one_value: str, lvl_two_key: str, lvl_two_value: str, mapping: typing.Dict, export_id: str):
        with self.__lock:
            try:
                m_hash = hash_mapping(mapping=mapping)
                self.__add_export(
                    export_id=export_id,
                    source=source,
                    lvl_one_key=lvl_one_key,
                    lvl_one_value=lvl_one_value,
                    lvl_two_value=lvl_two_value,
                    m_hash=m_hash
                )
                self.__add_mapping(mapping=mapping, m_hash=m_hash, export_id=export_id)
                self.__add_source(source=source, export_id=export_id)
                self.__add_msg_identifier(lvl_one_key=lvl_one_key, lvl_two_key=lvl_two_key, export_id=export_id)
                self.__add_filter(
                    lvl_one_value=lvl_one_value,
                    lvl_two_value=lvl_two_value,
                    m_hash=m_hash,
                    export_id=export_id
                )
                self.__updated = time.time_ns()
            except exceptions.FilterHandlerError as ex:
                logger.error(ex)
                if not isinstance(ex, exceptions.HashMappingError):
                    self.__del(export_id=export_id, update_time=False)

    def __del(self, export_id: str, update_time: bool = True):
        if export_id in self.__exports:
            try:
                export = self.__exports[export_id]
                self.__del_export(export_id=export_id)
                self.__del_mapping(m_hash=export[model.Filter.m_hash], export_id=export_id)
                self.__del_source(source=export[model.Filter.source], export_id=export_id)
                self.__del_msg_identifier(lvl_one_key=export[model.Filter.lvl_one_key], export_id=export_id)
                self.__del_filter(
                    lvl_one_value=export[model.Filter.lvl_one_value],
                    lvl_two_value=export[model.Filter.lvl_two_value],
                    m_hash=export[model.Filter.m_hash],
                    export_id=export_id
                )
                if update_time:
                    self.__updated = time.time_ns()
            except exceptions.FilterHandlerError as ex:
                logger.error(ex)

    def __del_with_lock(self, export_id: str):
        with self.__lock:
            self.__del(export_id=export_id)

    def __get_filters(self, lvl_one_value: str, lvl_two_value: str):
        try:
            return self.__msg_filters[lvl_one_value][lvl_two_value]
        except KeyError:
            raise exceptions.NoFilterError((lvl_one_value, lvl_two_value))

    def __identify_msg(self, keys: typing.Set, msg: typing.Dict):
        try:
            key = keys.intersection(set(msg))
            if not len(key) == 1:
                raise exceptions.NoMessageIdentifierError(set(msg))
            key = key.pop()
            return msg[key], msg[self.__msg_identifiers[key]]
        except Exception as ex:
            raise exceptions.MessageIdentificationError(ex)

    def filter_message(self, msg: typing.Dict, builder: typing.Callable[[typing.Generator], typing.Any] = builders.dict_builder):
        with self.__lock:
            filters = self.__get_filters(*self.__identify_msg(self.__msg_identifier_keys, msg))
            # data_sets = dict()
            # for mapping_id in filters:
            #     data = builder(mapper(self.__mappings[mapping_id], msg))
            #     for export_id in filters[mapping_id]:
            #         data_sets[export_id] = data
            data_sets = list()
            for mapping_id in filters:
                data_sets.append((builder(mapper(self.__mappings[mapping_id], msg)), tuple(filters[mapping_id])))
            return data_sets

    def start(self):
        self.__thread.start()

    def stop(self):
        self.__stop = True
        self.__thread.join()

    @property
    def sources(self):
        with self.__lock:
            return self.__sources.copy()

    @property
    def updated(self):
        with self.__lock:
            return self.__updated

    def run(self) -> None:
        while not self.__stop:
            try:
                start = time.time_ns()
                msg = self.__filter_consumer.get_filter()
                if msg:
                    try:
                        method = msg[model.FilterMessage.method]
                        if method == model.Methods.put:
                            self.__add(**msg[model.FilterMessage.payload])
                        elif method == model.Methods.delete:
                            self.__del_with_lock(**msg[model.FilterMessage.payload])
                        else:
                            raise exceptions.MethodError(method)
                    except Exception as ex:
                        logger.exception(ex)
                else:
                    duration = self.__fallback_delay * 1000000000 - (time.time_ns() - start)
                    if duration > 0:
                        time.sleep(duration / 1000000000)
            except Exception as ex:
                logger.exception(ex)
                time.sleep(self.__fallback_delay)
