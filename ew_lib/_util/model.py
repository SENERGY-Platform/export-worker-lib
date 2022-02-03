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

__all__ = ("Mapping", "Methods", "FilterMessage", "Filter")


class Mapping:
    src_path = "src_path"
    dst_path = "dst_path"
    type = "type"


class Methods:
    put = "put"
    delete = "delete"


class FilterMessage:
    method = "method"
    payload = "payload"


class Filter:
    source = "source"
    identifier_one_key = "identifier_one_key"
    identifier_one_val = "identifier_one_val"
    identifier_two_key = "identifier_two_key"
    identifier_two_val = "identifier_two_val"
    mapping = "mapping"
    m_hash = "m_hash"
