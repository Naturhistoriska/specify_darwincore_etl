
class ETLError(Exception):
    """Base exception for the ETL package."""
    pass

class ExtractionError(ETLError):
    """Raised during the extraction phase (download, unzip, XML parsing)."""
    pass

class TransformationError(ETLError):
    """Raised during the transformation phase (Dask processing)."""
    pass

class LoadingError(ETLError):
    """Raised during the loading phase."""
    pass
