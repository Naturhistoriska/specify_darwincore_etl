import logging

import dask.dataframe as dd

from etl.exceptions import TransformationError
from etl.extract import FieldMetadata, FileMetadata


def transform_extension_data(
    files: list[str],
    file_metadata: FileMetadata,
    indexed_header: list[str],
    default_fields_metadata: list[FieldMetadata],
    read_encoding: str = "utf-8",
    on_bad_lines: str = "warn",
    deduplicate_columns: list[str] | None = None,
) -> dd.DataFrame:
    """Transforms a list of files into a single Dask DataFrame.

    This includes reading, deduplicating, and mapping headers in a memory-efficient way.

    Args:
        files: A list of file paths to process.
        file_metadata: Metadata about the file type (delimiter, headers, etc.).
        indexed_header: The final column names mapped by index.
        default_fields_metadata: Fields to inject with default values if missing.
        read_encoding: The encoding for reading CSV files.
        on_bad_lines: Strategy for malformed CSV lines ('warn', 'error', 'skip').
        deduplicate_columns: Columns to consider for drop_duplicates. If None, all columns are used.

    Returns:
        A partitioned Dask DataFrame.
    """
    logging.info(f"Transforming {len(files)} files using Dask...")
    try:
        if not files:
            error_msg = "No files to transform."
            logging.error(error_msg)
            raise TransformationError(error_msg)

        # Dask reads files in parallel chunks.
        ddf = dd.read_csv(
            files,
            header=None,
            dtype=str,
            na_values=[],
            keep_default_na=False,
            blocksize=None,
            encoding=read_encoding,
            on_bad_lines=on_bad_lines,
            sep=file_metadata.fields_terminated_by.replace("\\t", "\t"),
            skiprows=file_metadata.ignore_header_lines,
        )

        if ddf is None or len(ddf.columns) == 0:
            error_msg = "No data read from files. Skipping."
            logging.error(error_msg)
            raise TransformationError(error_msg)

        # Deduplicate across the entire dataset.
        ddf = ddf.drop_duplicates(subset=deduplicate_columns if deduplicate_columns else None)

        if indexed_header and len(ddf.columns) == len(indexed_header):
            ddf.columns = indexed_header
        else:
            logging.warning(
                "Header length (%d) does not match number of columns (%d). Columns will not be renamed.",
                len(indexed_header),
                len(ddf.columns),
            )

        # Inject default values for missing columns
        for field in default_fields_metadata:
            if field.name is not None:
                if field.name not in ddf.columns:
                    ddf[field.name] = field.default
                else:
                    ddf[field.name] = ddf[field.name].fillna(field.default)

        return ddf  # type: ignore[no-any-return]
    except TransformationError:
        raise
    except Exception as e:
        error_msg = f"An unexpected error occurred during Dask transformation: {e}"
        logging.error(error_msg)
        raise TransformationError(error_msg) from e
