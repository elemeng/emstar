"""
emstar — Read, write, and inspect STAR files used in cryo-EM (RELION).

Designed as a friendlier alternative to ``starfile``:

- ``read()`` auto-returns DataFrames if ``polars`` or ``pandas`` is installed
- ``write()`` accepts both plain dicts and DataFrames
- Zero dependencies when used as a Rust library; Python bindings need only ``pyo3``

Usage::

    import emstar

    # Read — auto-detects polars > pandas > plain dict
    data = emstar.read("particles.star")
    data["particles"]  # pl.DataFrame | pd.DataFrame | dict[str, list]

    # Write — accepts dicts and DataFrames
    emstar.write(data, "out.star")
    emstar.write({"x": [1.0, 2.0]}, "out.star")

    # Inspect
    info = emstar.stats("particles.star")  # → dict
    emstar.validate("particles.star")      # → None (raises on error)
"""
from typing import Any, Dict, List, Union

# Type aliases for readability
SimpleBlock = Dict[str, Any]
LoopBlock = Dict[str, List[Any]]
StarData = Dict[str, Union[SimpleBlock, LoopBlock, "DataFrame"]]

class DataFrame:
    """Stand-in for polars.DataFrame or pandas.DataFrame."""
    pass

def read(path: str) -> Dict[str, Union["DataFrame", Dict[str, Any]]]:
    """Read a STAR file.

    Returns a dict of block name → data.
    Loop blocks become polars/pandas DataFrames if the library is installed,
    otherwise they are plain dicts of ``{column_name: [values...]}``.
    Simple blocks are always ``{key: value}`` dicts.

    Args:
        path: Path to the ``.star`` file.

    Returns:
        ``{block_name: DataFrame | dict}``

    Raises:
        FileNotFoundError: File does not exist.
        ValueError: File is malformed.
    """
    ...

def write(data: Dict[str, Any], path: str) -> None:
    """Write data to a STAR file.

    Accepts both plain dicts and DataFrames (polars/pandas).

    Args:
        data: Block dict as returned by ``read()``, or a plain
              ``{block_name: {column_name: [values...]}}`` dict.
        path: Output ``.star`` file path.

    Raises:
        ValueError: Data format is invalid.
    """
    ...

def stats(path: str) -> Dict[str, int]:
    """Get file statistics.

    Args:
        path: Path to the ``.star`` file.

    Returns:
        ``{"n_blocks": int, "n_simple": int, "n_loop": int, "total_loop_rows": int, "total_simple_entries": int}``

    Raises:
        FileNotFoundError: File does not exist.
    """
    ...

def validate(path: str) -> None:
    """Validate a STAR file. Raises an exception on invalid files.

    Args:
        path: Path to the ``.star`` file.

    Raises:
        FileNotFoundError: File does not exist.
        ValueError: File is malformed.
    """
    ...
