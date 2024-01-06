import json
import os
import sys
import voluptuous as vol
from .processors import utils
from . import const as const
from . import definitions as defs

DEFAULT_STORAGE_DIR = os.getenv("HOME")
RECENT_CACHE_FILENAME = "recent_{id}.json"

compile_storage_id = lambda cups: cups.lower()


def check_storage_integrity(data: defs.EdataData):
    """Check if an EdataData object follows a schema."""
    return defs.EdataSchema(data)


def load_storage(cups: str, storage_dir: str | None = None):
    """Load EdataData storage from its config dir."""
    if storage_dir is None:
        storage_dir = DEFAULT_STORAGE_DIR
    _subdir = os.path.join(storage_dir, "." + const.PROG_NAME)
    _recent_cache = os.path.join(
        _subdir, RECENT_CACHE_FILENAME.format(id=compile_storage_id(cups))
    )
    os.makedirs(_subdir, exist_ok=True)

    with open(_recent_cache) as f:
        return check_storage_integrity(utils.deserialize_dict(json.load(f)))


def dump_storage(cups: str, storage: defs.EdataData, storage_dir: str | None = None):
    if storage_dir is None:
        storage_dir = DEFAULT_STORAGE_DIR
    _subdir = os.path.join(storage_dir, "." + const.PROG_NAME)
    _recent_cache = os.path.join(
        _subdir, RECENT_CACHE_FILENAME.format(id=compile_storage_id(cups))
    )
    os.makedirs(_subdir, exist_ok=True)

    with open(_recent_cache, "w") as f:
        json.dump(utils.serialize_dict(check_storage_integrity(storage)), f)
