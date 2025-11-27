from typing import Any, Type, TypeVar

from pydantic import BaseModel
from sqlalchemy.types import JSON, TypeDecorator

T = TypeVar("T", bound=BaseModel)


class PydanticJSON(TypeDecorator):
    """
    Tipo de SQLAlchemy para guardar modelos Pydantic como JSON.
    Automáticamente serializa al guardar y deserializa al leer.
    """

    impl = JSON
    cache_ok = True

    def __init__(self, pydantic_model: Type[T]):
        super().__init__()
        self.pydantic_model = pydantic_model

    def process_bind_param(self, value: T | None, dialect: Any) -> Any:
        # Python -> Base de Datos
        if value is None:
            return None
        # Aquí ocurre la magia: mode='json' convierte datetime a string ISO
        return value.model_dump(mode="json")

    def process_result_value(self, value: Any, dialect: Any) -> T | None:
        # Base de Datos -> Python
        if value is None:
            return None
        # Reconstruimos el objeto Pydantic desde el diccionario
        return self.pydantic_model.model_validate(value)  # type: ignore
