"""The models module contains base classes used by other models."""
from humps import camelize
from pydantic import BaseModel
from pydantic import ConfigDict


class APIModel(BaseModel):
    """APIModel is a base class for models that are used for communicating with the Dapla Pseudo Service.

    It provides configuration for serializing/converting between camelCase (required by the API) and
    snake_case (used pythonically by this lib). It also provides some good defaults for converting a
    model to JSON.
    """

    model_config = ConfigDict(alias_generator=camelize, populate_by_name=True)

    def to_json(self) -> str:
        """Convert the model to JSON using camelCase aliases and only including assigned values."""
        return self.model_dump_json(exclude_unset=True, exclude_none=True, by_alias=True)
