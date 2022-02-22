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

from .._util import hash_dict, hash_list, get_value, logger, model, json_to_str, validate
from .. import exceptions
from .. import builders
import typing
import threading
import time

type_map = {
    "int": int,
    "float": float,
    "string": str,
    "bool": bool,
    "string_json": json_to_str
}


def hash_mapping(mapping: typing.Dict):
    try:
        return hash_dict(mapping)
    except Exception as ex:
        raise exceptions.HashMappingError(f"{ex} - {mapping}")


def parse_mapping(mapping: typing.Dict) -> typing.List[typing.Dict]:
    try:
        parsed_mapping = list()
        for key, value in mapping.items():
            validate(value, str, "source path")
            dst_path, val_type = key.split(":")
            validate(dst_path, str, "destination path")
            validate(val_type, str, "destination type")
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
        try:
            src_path = mapping[model.Mapping.src_path].split(".")
            yield mapping[model.Mapping.dst_path], type_map[mapping[model.Mapping.type]](get_value(src_path, msg, len(src_path) - 1))
        except Exception as ex:
            raise exceptions.MappingError(ex)


def validate_identifier(key: str, value: typing.Optional[typing.Union[str, int, float]] = None):
    validate(key, str, "identifier key")
    if value:
        validate(value, (str, int, float), "identifier value")
    return key, value


class FilterHandler:
    __log_msg_prefix = "filter handler"
    __log_err_msg_prefix = f"{__log_msg_prefix} error"

    def __init__(self):
        self.__lock = threading.Lock()
        self.__msg_identifiers = dict()
        self.__msg_filters = dict()
        self.__mappings = dict()
        self.__sources = set()
        self.__exports = dict()
        self.__mapping_export_map = dict()
        self.__msg_identifiers_export_map = dict()
        self.__sources_export_map = dict()
        self.__sources_timestamp = None

    def __add_filter(self, i_str, m_hash, export_id):
        try:
            try:
                self.__msg_filters[i_str][m_hash].add(export_id)
            except KeyError:
                if i_str not in self.__msg_filters:
                    self.__msg_filters[i_str] = dict()
                if m_hash not in self.__msg_filters[i_str]:
                    self.__msg_filters[i_str][m_hash] = {export_id}
        except Exception as ex:
            raise exceptions.AddFilterError(ex)

    def __del_filter(self, i_str, m_hash, export_id):
        try:
            self.__msg_filters[i_str][m_hash].discard(export_id)
            if not self.__msg_filters[i_str][m_hash]:
                del self.__msg_filters[i_str][m_hash]
                if not self.__msg_filters[i_str]:
                    del self.__msg_filters[i_str]
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

    def __add_msg_identifier(self, identifiers: list, export_id: str):
        try:
            i_val_keys = list()
            i_no_val_keys = list()
            i_values = list()
            for identifier in identifiers:
                key, value = validate_identifier(**identifier)
                if value:
                    i_val_keys.append(key)
                    i_values.append(value)
                else:
                    i_no_val_keys.append(key)
            i_val_keys.sort()
            i_no_val_keys.sort()
            i_values.sort()
            i_keys = i_val_keys + i_no_val_keys
            i_hash = hash_list(i_keys)
            if i_hash not in self.__msg_identifiers:
                self.__msg_identifiers[i_hash] = (set(i_keys), i_val_keys, "".join(i_no_val_keys), len(i_keys))
            if i_hash not in self.__msg_identifiers_export_map:
                self.__msg_identifiers_export_map[i_hash] = {export_id}
            else:
                self.__msg_identifiers_export_map[i_hash].add(export_id)
            return i_hash, "".join(i_values) + self.__msg_identifiers[i_hash][2]
        except Exception as ex:
            raise exceptions.AddMessageIdentifierError(ex)

    def __del_msg_identifier(self, i_hash: str, export_id: str):
        try:
            self.__msg_identifiers_export_map[i_hash].discard(export_id)
            if not self.__msg_identifiers_export_map[i_hash]:
                del self.__msg_identifiers[i_hash]
                del self.__msg_identifiers_export_map[i_hash]
        except Exception as ex:
            raise exceptions.DeleteMessageIdentifierError(ex)

    def __add_source(self, source: str, export_id: str):
        try:
            self.__sources.add(source)
            if source not in self.__sources_export_map:
                self.__sources_export_map[source] = {export_id}
            else:
                self.__sources_export_map[source].add(export_id)
            self.__sources_timestamp = time.time_ns()
        except Exception as ex:
            raise exceptions.AddSourceError(ex)

    def __del_source(self, source: str, export_id: str):
        try:
            self.__sources_export_map[source].discard(export_id)
            if not self.__sources_export_map[source]:
                self.__sources.discard(source)
                del self.__sources_export_map[source]
                self.__sources_timestamp = time.time_ns()
        except Exception as ex:
            raise exceptions.DeleteSourceError(ex)

    def __add_export(self, export_id: str, source: str, m_hash: str, i_hash: str, i_str: str):
        try:
            self.__exports[export_id] = {
                model.Export.source: source,
                model.Export.m_hash: m_hash,
                model.Export.i_hash: i_hash,
                model.Export.i_str: i_str
            }
        except Exception as ex:
            raise exceptions.AddExportError(ex)

    def __del_export(self, export_id: str):
        try:
            del self.__exports[export_id]
        except Exception as ex:
            raise exceptions.DeleteExportError(ex)

    def __add(self, source: str, mapping: typing.Dict, export_id: str, identifiers: typing.Optional[list] = None):
        validate(source, str, model.FilterMessagePayload.source)
        validate(mapping, dict, model.FilterMessagePayload.mapping)
        validate(export_id, str, model.FilterMessagePayload.export_id)
        if identifiers:
            validate(identifiers, list, model.FilterMessagePayload.identifiers)
        with self.__lock:
            m_hash = hash_mapping(mapping=mapping)
            if identifiers:
                i_hash, i_str = self.__add_msg_identifier(identifiers=identifiers, export_id=export_id)
            else:
                i_hash = None
                i_str = source
            self.__add_export(
                export_id=export_id,
                source=source,
                m_hash=m_hash,
                i_hash=i_hash,
                i_str=i_str
            )
            self.__add_mapping(mapping=mapping, m_hash=m_hash, export_id=export_id)
            self.__add_source(source=source, export_id=export_id)
            self.__add_filter(
                i_str=i_str,
                m_hash=m_hash,
                export_id=export_id
            )
            self.__sources_timestamp = time.time_ns()

    def __del(self, export_id: str):
        with self.__lock:
            if export_id in self.__exports:
                export = self.__exports[export_id]
                self.__del_export(export_id=export_id)
                if export[model.Export.i_hash]:
                    self.__del_msg_identifier(i_hash=export[model.Export.i_hash], export_id=export_id)
                self.__del_mapping(m_hash=export[model.Export.m_hash], export_id=export_id)
                self.__del_source(source=export[model.Export.source], export_id=export_id)
                self.__del_filter(
                    i_str=export[model.Export.i_str],
                    m_hash=export[model.Export.m_hash],
                    export_id=export_id
                )

    def __identify_msg(self, msg: typing.Dict):
        try:
            msg_keys = set(msg.keys())
            identifier = None
            for i_hash in self.__msg_identifiers:
                if self.__msg_identifiers[i_hash][0].issubset(msg_keys):
                    if not identifier:
                        identifier = self.__msg_identifiers[i_hash]
                    else:
                        if self.__msg_identifiers[i_hash][3] > identifier[3]:
                            identifier = self.__msg_identifiers[i_hash]
            if identifier:
                return "".join([str(msg[key]) for key in identifier[1]]) + identifier[2]
        except Exception as ex:
            raise exceptions.MessageIdentificationError(ex)

    def filter_message(self, message: typing.Dict, source: typing.Optional[str] = None, builder: typing.Optional[typing.Callable[[typing.Generator], typing.Any]] = builders.dict_builder):
        with self.__lock:
            i_str = self.__identify_msg(msg=message) or source
            data_sets = list()
            if i_str in self.__msg_filters:
                try:
                    for mapping_id in self.__msg_filters[i_str]:
                        data_sets.append(
                            (
                                builder(mapper(self.__mappings[mapping_id], message)),
                                tuple(self.__msg_filters[i_str][mapping_id])
                            )
                        )
                except Exception as ex:
                    raise exceptions.FilterMessageError(ex)
            else:
                raise exceptions.NoFilterError(message)
            return data_sets

    def add(self, filter: typing.Dict):
        validate(filter, dict, "filter")
        self.__add(**filter)

    def delete(self, export_id: str):
        validate(export_id, str, "export_id")
        self.__del(export_id=export_id)

    @property
    def sources(self) -> list:
        with self.__lock:
            return list(self.__sources)

    @property
    def sources_timestamp(self) -> str:
        with self.__lock:
            return self.__sources_timestamp

