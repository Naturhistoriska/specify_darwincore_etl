from pathlib import Path

from pydantic import BaseModel, Field, HttpUrl, field_validator


class ProjectConfig(BaseModel):
    zip_path: Path
    extract_dir: Path
    output_dir: Path
    url: HttpUrl
    output_separator: str = "\t"
    output_extension: str = ".txt"
    download_retries: int = Field(default=3, ge=0)
    download_backoff_factor: float = Field(default=0.3, ge=0)
    log_format: str = Field(default="text")
    read_encoding: str = "utf-8"
    write_encoding: str = "utf-8"
    on_bad_lines: str = "warn"
    deduplicate_columns: list[str] = Field(default_factory=list)

    @field_validator("on_bad_lines")
    @classmethod
    def validate_on_bad_lines(cls, v: str) -> str:
        if v not in {"warn", "error", "skip"}:
            raise ValueError("on_bad_lines must be one of 'warn', 'error', 'skip'")
        return v

    @field_validator("log_format")
    @classmethod
    def validate_log_format(cls, v: str) -> str:
        if v not in {"text", "json"}:
            raise ValueError("log_format must be one of 'text', 'json'")
        return v
