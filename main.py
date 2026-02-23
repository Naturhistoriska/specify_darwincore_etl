import argparse
import logging
import sys
import time
from pathlib import Path
from typing import Any

import yaml
from tqdm import tqdm

from etl.config_schema import ProjectConfig
from etl.exceptions import ETLError
from etl.extract import FileMetadata, download_data, extract_archive, parse_meta_xml
from etl.load import save_dataframe_to_file
from etl.logging_config import setup_logging
from etl.patches import apply_dwcahandler_patches
from etl.transform import transform_extension_data


class ETLPipeline:
    """Encapsulates the orchestration logic for the Specify Darwin Core ETL process."""

    def __init__(self, config: ProjectConfig):
        self.config = config

    def _prepare_directories(self) -> None:
        """Ensures all necessary directories exist."""
        self.config.zip_path.parent.mkdir(parents=True, exist_ok=True)
        self.config.extract_dir.mkdir(parents=True, exist_ok=True)
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def _determine_core_id_column(self, archive_metadata: Any) -> str | None:
        """Determines the name of the core ID column from metadata."""
        core_id_name = None
        if archive_metadata.core.id_index is not None:
            for field in archive_metadata.core.fields:
                if field.index == archive_metadata.core.id_index:
                    core_id_name = field.name
                    break

        if core_id_name:
            return str(core_id_name)
        return "id" if archive_metadata.core.id_index is not None else None

    def _run_download_phase(self) -> None:
        """Downloads the archive zip from the configured URL."""
        start_time = time.time()
        download_data(
            str(self.config.url),
            self.config.zip_path,
            retries=self.config.download_retries,
            backoff_factor=self.config.download_backoff_factor,
        )
        logging.info(f"Download phase completed in {time.time() - start_time:.2f} seconds")

    def _run_unzip_phase(self) -> None:
        """Extracts the configured archive zip into extract_dir."""
        start_time = time.time()
        if not self.config.zip_path.exists():
            raise ETLError(f"Archive not found at {self.config.zip_path}")
        extract_archive(self.config.zip_path, self.config.extract_dir)
        logging.info(f"Unzip phase completed in {time.time() - start_time:.2f} seconds")

    def _run_processing_phase(self) -> None:
        """Parses metadata and processes core/extension files."""
        meta_path = self.config.extract_dir / "meta.xml"
        if not meta_path.exists():
            raise ETLError(f"meta.xml not found in {self.config.extract_dir}")

        archive_metadata = parse_meta_xml(meta_path)
        core_id_column = self._determine_core_id_column(archive_metadata)
        files_to_process = [archive_metadata.core] + archive_metadata.extensions

        total_start_time = time.time()
        for file_metadata in tqdm(files_to_process, desc="Processing DWCA files"):
            self._process_file(file_metadata, core_id_column)

        logging.info(
            f"All files processed in {time.time() - total_start_time:.2f} seconds. "
            f"Outputs in {self.config.output_dir}"
        )

    def run(self, mode: str = "all") -> None:
        """Executes the ETL pipeline for the selected mode."""
        try:
            apply_dwcahandler_patches()
            self._prepare_directories()

            if mode in ("all", "download"):
                self._run_download_phase()
                self._run_unzip_phase()
                if mode == "download":
                    logging.info("Download mode complete; skipping processing phase.")

            if mode in ("all", "process"):
                if mode == "process":
                    if self.config.zip_path.exists():
                        logging.info(
                            "Process mode: archive %s found; extracting into %s before processing",
                            self.config.zip_path,
                            self.config.extract_dir,
                        )
                        self._run_unzip_phase()
                    else:
                        logging.info(
                            "Process mode: archive not found at %s; processing existing extracted files in %s",
                            self.config.zip_path,
                            self.config.extract_dir,
                        )
                self._run_processing_phase()

        except ETLError as e:
            logging.error(f"ETL pipeline failed: {e}")
            sys.exit(1)
        except Exception as e:
            logging.critical(f"An unexpected error occurred: {e}")
            sys.exit(1)

    def _process_file(self, file_metadata: FileMetadata, core_id_column: str | None) -> None:
        """Processes a single Darwin Core file (core or extension)."""
        basename = file_metadata.file_path.name
        logging.info("Processing file: %s", basename)
        start_time = time.time()

        indexed_header, default_metadata = file_metadata.get_header(core_id_column_name=core_id_column)

        # Collect all chunks matching the file pattern
        pattern = f"{file_metadata.file_path.stem}*{file_metadata.file_path.suffix}"
        files = [str(f) for f in self.config.extract_dir.glob(pattern)]

        if not files:
            logging.warning(f"No files found for {basename} matching {pattern}. Skipping.")
            return

        df = transform_extension_data(
            files,
            file_metadata,
            indexed_header,
            default_metadata,
            read_encoding=self.config.read_encoding,
            on_bad_lines=self.config.on_bad_lines,
            deduplicate_columns=self.config.deduplicate_columns,
        )

        row_count = len(df)
        output_path = self.config.output_dir / f"{file_metadata.file_path.stem}{self.config.output_extension}"

        save_dataframe_to_file(
            df, str(output_path), sep=self.config.output_separator, encoding=self.config.write_encoding
        )

        logging.info(f"Processed {row_count} rows from {basename} in {time.time() - start_time:.2f} seconds")


def main() -> None:
    """CLI entry point for the ETL pipeline."""
    parser = argparse.ArgumentParser(description="Specify Darwin Core ETL Pipeline")
    parser.add_argument("config", help="Path to the YAML configuration file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level",
    )
    parser.add_argument(
        "--mode",
        default="all",
        choices=["all", "download", "process"],
        help="Pipeline run mode: run all phases, only download/unzip, or only process extracted data.",
    )
    args = parser.parse_args()

    try:
        config_path = Path(args.config)
        with config_path.open() as f:
            raw_config = yaml.safe_load(f)
        config = ProjectConfig(**raw_config)
    except Exception as e:
        print(f"Failed to load configuration: {e}", file=sys.stderr)
        sys.exit(1)

    setup_logging(log_level=args.log_level, log_format=config.log_format)

    pipeline = ETLPipeline(config)
    pipeline.run(mode=args.mode)


if __name__ == "__main__":
    main()
