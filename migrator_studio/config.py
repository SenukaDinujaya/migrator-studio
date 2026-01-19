from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Optional

_DEFAULT_DATA_PATH = "./data"
_ENV_VAR_NAME = "MIGRATOR_STUDIO_DATA_PATH"


@dataclass
class Config:
    data_path: str


_config: Optional[Config] = None


def configure(data_path: Optional[str] = None) -> None:
    """
    Configure the data path for loading source files.

    Priority:
    1. Explicit data_path argument
    2. MIGRATOR_STUDIO_DATA_PATH environment variable
    3. Default "./data"
    """
    global _config

    if data_path is not None:
        resolved_path = data_path
    else:
        resolved_path = os.environ.get(_ENV_VAR_NAME, _DEFAULT_DATA_PATH)

    _config = Config(data_path=resolved_path)


def get_config() -> Config:
    global _config
    if _config is None:
        configure()
    return _config


def reset_config() -> None:
    global _config
    _config = None
