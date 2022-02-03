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


class FilterHandlerError(Exception):
    pass


class NoMessageIdentifierError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"missing identifiers for message with keys: {arg}")


class MessageIdentificationError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"message identification failed: {arg}")


class MethodError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"unknown method: {arg}")


class NoFilterError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"no filters for: {arg}")


class FilterMessageError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"filtering message failed: {arg}")


class MappingError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"mapping error: {arg}")


class MessageIdentifierMissmatchError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"provided identifier already exists with different value: {arg}")


class HashMappingError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"hashing mapping failed: {arg}")


class ParseMappingError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"parsing mapping failed: {arg}")


class AddFilterError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"adding filter failed: {arg}")


class DeleteFilterError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"deleting filter failed: {arg}")


class AddMappingError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"adding mapping failed: {arg}")


class DeleteMappingError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"deleting mapping failed: {arg}")


class AddSourceError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"adding source failed: {arg}")


class DeleteSourceError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"deleting source failed: {arg}")


class AddMessageIdentifierError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"adding message identifier failed: {arg}")


class DeleteMessageIdentifierError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"deleting message identifier failed: {arg}")


class AddExportError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"adding export failed: {arg}")


class DeleteExportError(FilterHandlerError):
    def __init__(self, arg):
        super().__init__(f"deleting export failed: {arg}")


class KafkaMessageError(Exception):
    __text = "kafka message error"

    def __init__(self, arg, prefix=None):
        super().__init__(self.gen_text(arg=arg, prefix=prefix))

    @staticmethod
    def gen_text(arg, prefix=None):
        if prefix:
            return f"{prefix}: {KafkaMessageError.__text}: {arg}"
        else:
            return f"{KafkaMessageError.__text}: {arg}"
