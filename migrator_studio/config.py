from __future__ import annotations

import os
from dataclasses import dataclass

_DEFAULT_DATA_PATH = "./data"
_DEFAULT_NOTEBOOK_DIR = ".tmp"
_ENV_VAR_DATA_PATH = "MIGRATOR_STUDIO_DATA_PATH"
_ENV_VAR_NOTEBOOK_DIR = "MIGRATOR_STUDIO_NOTEBOOK_DIR"


@dataclass
class Config:
    data_path: str
    notebook_dir: str


_config: Config | None = None


def configure(
    data_path: str | None = None,
    notebook_dir: str | None = None,
) -> None:
    """
    Configure migrator_studio settings.

    Args:
        data_path: Path to source data files (.feather files).
            Priority: argument > MIGRATOR_STUDIO_DATA_PATH env > "./data"
        notebook_dir: Directory for generated notebooks.
            Priority: argument > MIGRATOR_STUDIO_NOTEBOOK_DIR env > ".tmp"
            If relative, resolved relative to data_path.
    """
    global _config

    # Resolve data path
    if data_path is not None:
        resolved_data_path = data_path
    else:
        resolved_data_path = os.environ.get(_ENV_VAR_DATA_PATH, _DEFAULT_DATA_PATH)

    # Resolve notebook directory
    if notebook_dir is not None:
        resolved_notebook_dir = notebook_dir
    else:
        resolved_notebook_dir = os.environ.get(_ENV_VAR_NOTEBOOK_DIR, _DEFAULT_NOTEBOOK_DIR)

    # If notebook_dir is relative, make it relative to data_path
    if not os.path.isabs(resolved_notebook_dir):
        resolved_notebook_dir = os.path.join(resolved_data_path, resolved_notebook_dir)

    _config = Config(
        data_path=resolved_data_path,
        notebook_dir=resolved_notebook_dir,
    )


def get_config() -> Config:
    global _config
    if _config is None:
        configure()
    return _config


def reset_config() -> None:
    global _config
    _config = None
