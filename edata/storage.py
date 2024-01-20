import json
import logging
import os
from .processors import utils
from . import const as const
from . import definitions as defs

_LOGGER = logging.getLogger(__name__)

DEFAULT_STORAGE_DIR = os.getenv("HOME")
RECENT_CACHE_FILENAME = "edata_{id}.json"

compile_storage_id = lambda cups: cups.lower()


def check_storage_integrity(data: defs.EdataData):
    """Check if an EdataData object follows a schema."""
    return defs.EdataSchema(data)


def load_storage(cups: str, storage_dir: str | None = None):
    """Load EdataData storage from its config dir."""
    if storage_dir is None:
        storage_dir = DEFAULT_STORAGE_DIR
    _subdir = os.path.join(storage_dir, const.PROG_NAME)
    _recent_cache = os.path.join(
        _subdir, RECENT_CACHE_FILENAME.format(id=compile_storage_id(cups))
    )
    os.makedirs(_subdir, exist_ok=True)

    with open(_recent_cache, encoding="utf-8") as f:
        return check_storage_integrity(utils.deserialize_dict(json.load(f)))


def dump_storage(cups: str, storage: defs.EdataData, storage_dir: str | None = None):
    """Update EdataData storage."""
    if storage_dir is None:
        storage_dir = DEFAULT_STORAGE_DIR
    _subdir = os.path.join(storage_dir, const.PROG_NAME)
    _recent_cache = os.path.join(
        _subdir, RECENT_CACHE_FILENAME.format(id=compile_storage_id(cups))
    )
    os.makedirs(_subdir, exist_ok=True)

    with open(_recent_cache, "w", encoding="utf-8") as f:
        json.dump(utils.serialize_dict(check_storage_integrity(storage)), f)
