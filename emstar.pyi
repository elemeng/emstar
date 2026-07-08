from typing import Any, Dict, List, Optional, Union

class _DataFrame:
    """Stand-in for polars.DataFrame or pandas.DataFrame."""
    pass

def read(path: str) -> Dict[str, Union[_DataFrame, Dict[str, Any]]]:
    """Read a STAR file. Returns DataFrames if polars/pandas is installed.
    
    Simple blocks become dicts; loop blocks become polars/pandas DataFrames
    (or plain dicts of lists as fallback).
    """
    ...

def write(data: Dict[str, Any], path: str) -> None:
    """Write data to a STAR file. Accepts dicts and DataFrames."""
    ...

def stats(path: str) -> Dict[str, int]:
    """Get file statistics.
    
    Returns dict with keys: n_blocks, n_simple, n_loop,
    total_loop_rows, total_simple_entries.
    """
    ...

def validate(path: str) -> None:
    """Validate a STAR file. Raises ValueError on invalid files."""
    ...
