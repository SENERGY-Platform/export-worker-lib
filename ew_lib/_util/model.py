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

__all__ = ("Methods", "FilterMessage", "FilterMessagePayload", "Mapping", "Identifier", "Export")


class Methods:
    put = "put"
    delete = "delete"


class FilterMessage:
    method = "method"
    payload = "payload"


class FilterMessagePayload:
    source = "source"
    identifiers = "identifiers"
    mapping = "mapping"
    export_id = "export_id"


class Mapping:
    src_path = "src_path"
    dst_path = "dst_path"
    type = "type"


class Identifier:
    key = "key"
    value = "value"


class Export:
    source = "source"
    m_hash = "m_hash"
    i_hash = "i_hash"
    i_str = "i_str"
