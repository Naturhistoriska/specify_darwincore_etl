import logging
import time
import zipfile
from pathlib import Path

import requests
from dwcahandler.dwca import MetaDwCA

from etl.exceptions import ExtractionError


class FieldMetadata:
    index: int | None
    term: str
    name: str | None
    default: str | None

    def __init__(self, index: int | None, term: str, default: str | None = None):
        self.index = index
        self.term = term
        self.name = term.split("/")[-1] if term else None
        self.default = default


class FileMetadata:
    def __init__(
        self,
        file_path: Path,
        row_type: str,
        fields: list[FieldMetadata],
        fields_terminated_by: str,
        ignore_header_lines: int,
        id_index: int | None = None,
        coreid_index: int | None = None,
    ):
        self.file_path = file_path
        self.row_type = row_type
        self.fields = fields
        self.fields_terminated_by = fields_terminated_by
        self.ignore_header_lines = ignore_header_lines
        self.id_index = id_index
        self.coreid_index = coreid_index  # For extension file

    def get_header(self, core_id_column_name: str | None = None) -> tuple[list[str], list[FieldMetadata]]:
        header_map: dict[int, str] = {}
        max_index = -1
        default_fields_metadata: list[FieldMetadata] = []

        for field in self.fields:
            if field.index is not None:
                header_map[field.index] = field.name or ""
                max_index = max(max_index, field.index)
            elif field.default is not None and field.name is not None:
                default_fields_metadata.append(field)

        if core_id_column_name is not None:
            if self.coreid_index is not None:  # This is for extension files
                header_map[self.coreid_index] = core_id_column_name
                max_index = max(max_index, self.coreid_index)
            elif self.id_index is not None:  # This is for the core file itself
                # Check if id_index is already mapped by a field. If not, add core_id_column_name.
                if self.id_index not in header_map:
                    header_map[self.id_index] = core_id_column_name
                max_index = max(max_index, self.id_index)

        indexed_header = [header_map.get(i, "") for i in range(max_index + 1)]
        return indexed_header, default_fields_metadata


class ArchiveMetadata:
    def __init__(self, core: FileMetadata, extensions: list[FileMetadata]):
        self.core = core
        self.extensions = extensions


def download_data(url: str, dest: Path, retries: int = 3, backoff_factor: float = 0.3) -> None:
    """Downloads a file from a URL, with retries on failure."""
    logging.info(f"Downloading from {url}...")
    for i in range(retries):
        try:
            response = requests.get(url, stream=True, timeout=30)
            response.raise_for_status()
            with dest.open("wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            logging.info(f"Download complete: {dest}")
            return
        except requests.exceptions.RequestException as e:
            if i < retries - 1:
                sleep_time = backoff_factor * (2**i)
                logging.warning(f"Download failed (attempt {i + 1}/{retries}). Retrying in {sleep_time:.2f} seconds...")
                time.sleep(sleep_time)
            else:
                error_msg = f"Failed to download {url} after {retries} attempts: {e}"
                logging.error(error_msg)
                raise ExtractionError(error_msg) from e


def extract_archive(zip_path: Path, out_dir: Path) -> None:
    """Extracts all files from a zip archive to a destination directory."""
    logging.info(f"Extracting {zip_path} to {out_dir}...")
    try:
        if not zip_path.exists():
            error_msg = f"ZIP file not found: {zip_path}"
            logging.error(error_msg)
            raise ExtractionError(error_msg)

        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(out_dir)

        logging.info(f"Extraction complete: all files extracted to {out_dir}")
    except zipfile.BadZipFile as e:
        error_msg = f"{zip_path} is not a valid zip file."
        logging.error(f"Error: {error_msg}")
        raise ExtractionError(error_msg) from e
    except Exception as e:
        error_msg = f"An unexpected error occurred during extraction: {e}"
        logging.error(error_msg)
        raise ExtractionError(error_msg) from e


def map_dwca_metadata(meta_dwca: MetaDwCA, archive_dir: Path) -> ArchiveMetadata:
    """Maps dwcahandler's metadata objects to internal FileMetadata and ArchiveMetadata."""
    core_meta = None
    extensions_meta = []

    for element in meta_dwca.meta_elements:
        fields = []
        for f in element.fields:
            fields.append(
                FieldMetadata(
                    index=int(f.index) if f.index is not None else None,
                    term=f.term if f.term else f.field_name,
                    default=f.default,
                )
            )

        file_path = archive_dir / element.meta_element_type.file_name
        delimiter = element.meta_element_type.csv_encoding.csv_delimiter
        ignore_header_lines = int(element.meta_element_type.ignore_header_lines or 0)

        id_index = None
        if element.core_id and element.core_id.index is not None:
            id_index = int(element.core_id.index)

        file_metadata = FileMetadata(
            file_path=file_path,
            row_type=element.meta_element_type.type.value if element.meta_element_type.type else "",
            fields=fields,
            fields_terminated_by=delimiter,
            ignore_header_lines=ignore_header_lines,
            id_index=id_index if element.meta_element_type.core_or_ext_type.value == "core" else None,
            coreid_index=id_index if element.meta_element_type.core_or_ext_type.value == "extension" else None,
        )

        if element.meta_element_type.core_or_ext_type.value == "core":
            core_meta = file_metadata
        else:
            extensions_meta.append(file_metadata)

    if not core_meta:
        raise ExtractionError("No core file metadata found in meta.xml.")

    return ArchiveMetadata(core_meta, extensions_meta)


def parse_meta_xml(meta_path: Path) -> ArchiveMetadata:
    """Parses meta.xml for archive metadata using dwcahandler's MetaDwCA."""
    logging.info(f"Parsing {meta_path} for archive metadata using dwcahandler...")
    try:
        meta_dwca = MetaDwCA()
        meta_dwca.read_meta_file(str(meta_path))

        if not meta_dwca.meta_elements:
            raise ExtractionError("No metadata elements found in meta.xml.")

        return map_dwca_metadata(meta_dwca, meta_path.parent)

    except Exception as e:
        error_msg = f"An unexpected error occurred during meta parsing: {e}"
        logging.error(error_msg)
        raise ExtractionError(error_msg) from e
