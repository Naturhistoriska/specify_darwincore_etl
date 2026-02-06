import logging

import dask.dataframe as dd
from dask.diagnostics import ProgressBar  # type: ignore[attr-defined]

from etl.exceptions import LoadingError


def save_dataframe_to_file(ddf: dd.DataFrame, output_path: str, sep: str = "\t", encoding: str = "utf-8") -> None:
    """Saves a Dask DataFrame to a single file.

    Args:
        ddf: The Dask DataFrame to save.
        output_path: The path where the file should be saved.
        sep: The separator to use in the output file.
        encoding: The encoding to use for the output file.

    Raises:
        LoadingError: If an error occurred during loading.
    """
    logging.info(f"Saving data to {output_path}...")
    try:
        # We want to save the entire DataFrame as a single file.
        # to_csv with single_file=True is only supported for some storage backends,
        # otherwise we'd need to collapse partitions which might be memory-intensive.
        # For local files, we can use compute() or dask's own single file handling.
        with ProgressBar():  # type: ignore[no-untyped-call]
            ddf.to_csv(  # type: ignore[no-untyped-call]
                output_path,
                sep=sep,
                index=False,
                encoding=encoding,
                single_file=True,
            )
        logging.info(f"Successfully created: {output_path}")
    except Exception as e:
        error_msg = f"An unexpected error occurred during loading: {e}"
        logging.error(error_msg)
        raise LoadingError(error_msg) from e
