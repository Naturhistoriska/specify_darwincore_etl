from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from etl.config_schema import ProjectConfig


def test_valid_config() -> None:
    raw_config: dict[str, Any] = {
        "zip_path": "data/test.zip",
        "extract_dir": "data/extracted",
        "output_dir": "data/output",
        "url": "http://example.com/data.zip",
        "log_format": "json",
        "on_bad_lines": "skip",
    }
    config = ProjectConfig(**raw_config)
    assert config.zip_path == Path("data/test.zip")
    assert str(config.url) == "http://example.com/data.zip"
    assert config.log_format == "json"
    assert config.on_bad_lines == "skip"


def test_invalid_url() -> None:
    raw_config: dict[str, Any] = {
        "zip_path": "data/test.zip",
        "extract_dir": "data/extracted",
        "output_dir": "data/output",
        "url": "not-a-url",
    }
    with pytest.raises(ValidationError) as excinfo:
        ProjectConfig(**raw_config)
    assert "url" in str(excinfo.value)


def test_invalid_on_bad_lines() -> None:
    raw_config: dict[str, Any] = {
        "zip_path": "data/test.zip",
        "extract_dir": "data/extracted",
        "output_dir": "data/output",
        "url": "http://example.com/data.zip",
        "on_bad_lines": "invalid-option",
    }
    with pytest.raises(ValidationError) as excinfo:
        ProjectConfig(**raw_config)
    assert "on_bad_lines" in str(excinfo.value)


def test_default_values() -> None:
    raw_config: dict[str, Any] = {
        "zip_path": "data/test.zip",
        "extract_dir": "data/extracted",
        "output_dir": "data/output",
        "url": "http://example.com/data.zip",
    }
    config = ProjectConfig(**raw_config)
    assert config.output_separator == "\t"
    assert config.output_extension == ".txt"
    assert config.download_retries == 3
    assert config.log_format == "text"
