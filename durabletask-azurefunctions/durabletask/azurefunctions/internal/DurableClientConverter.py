import abc
from typing import Any, Optional

from azure.functions import meta


class DurableInConverter(meta._BaseConverter, binding=None):

    @classmethod
    @abc.abstractmethod
    def check_input_type_annotation(cls, pytype: type) -> bool:
        pass

    @classmethod
    @abc.abstractmethod
    def decode(cls, data: meta.Datum, *, trigger_metadata) -> Any:
        raise NotImplementedError

    @classmethod
    @abc.abstractmethod
    def has_implicit_output(cls) -> bool:
        return False


class DurableOutConverter(meta._BaseConverter, binding=None):

    @classmethod
    @abc.abstractmethod
    def check_output_type_annotation(cls, pytype: type) -> bool:
        pass

    @classmethod
    @abc.abstractmethod
    def encode(cls, obj: Any, *,
               expected_type: Optional[type]) -> Optional[meta.Datum]:
        raise NotImplementedError

# Durable Functions Durable Client Bindings


class DurableClientConverter(DurableInConverter,
                             DurableOutConverter,
                             binding='durableClient'):
    @classmethod
    def has_implicit_output(cls) -> bool:
        return False
