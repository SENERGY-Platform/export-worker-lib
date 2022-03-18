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

import traceback


class FilterHandlerError(Exception):
    pass


class MessageIdentificationError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"message identification failed: reason={traceback.format_exception_only(type(ex), ex)}")


class NoFilterError(FilterHandlerError):
    def __init__(self, msg):
        super().__init__(f"no filter for: message={msg}")


class FilterMessageError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"filtering message failed: reason={traceback.format_exception_only(type(ex), ex)}")


class MappingError(FilterHandlerError):
    def __init__(self, ex, mapping):
        super().__init__(f"mapping error: reason={traceback.format_exception_only(type(ex), ex)} mapping={mapping}")


class HashMappingError(FilterHandlerError):
    def __init__(self, ex, mappings):
        super().__init__(f"hashing mapping failed: reason={traceback.format_exception_only(type(ex), ex)} mappings={mappings}")


class ParseMappingError(FilterHandlerError):
    def __init__(self, ex, mappings):
        super().__init__(f"parsing mapping failed: reason={traceback.format_exception_only(type(ex), ex)} mappings={mappings}")


class AddFilterError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"adding filter failed: reason={traceback.format_exception_only(type(ex), ex)}")


class DeleteFilterError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"deleting filter failed: reason={traceback.format_exception_only(type(ex), ex)}")


class AddMappingError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"adding mapping failed: reason={traceback.format_exception_only(type(ex), ex)}")


class DeleteMappingError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"deleting mapping failed: reason={traceback.format_exception_only(type(ex), ex)}")


class AddSourceError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"adding source failed: reason={traceback.format_exception_only(type(ex), ex)}")


class DeleteSourceError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"deleting source failed: reason={traceback.format_exception_only(type(ex), ex)}")


class AddMessageIdentifierError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"adding message identifier failed: reason={traceback.format_exception_only(type(ex), ex)}")


class DeleteMessageIdentifierError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"deleting message identifier failed: reason={traceback.format_exception_only(type(ex), ex)}")


class AddExportError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"adding export failed: reason={traceback.format_exception_only(type(ex), ex)}")


class DeleteExportError(FilterHandlerError):
    def __init__(self, ex):
        super().__init__(f"deleting export failed: reason={traceback.format_exception_only(type(ex), ex)}")
