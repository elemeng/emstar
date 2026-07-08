"""
emstar — read, write, and inspect STAR files used in cryo-EM (RELION).

Simple blocks → ``dict``, loop blocks → ``polars.DataFrame`` / ``pandas.DataFrame`` / ``dict`` of lists.

Examples
--------
>>> import emstar
>>> data = emstar.read("particles.star")
>>> data["particles"]                     # DataFrame or dict of lists
>>> emstar.write(data, "output.star")     # write back
>>> emstar.stats("particles.star")        # file statistics
>>> emstar.validate("particles.star")     # raise on invalid

Functions
---------
read      Read a STAR file into a dict of blocks.
write     Write blocks to a STAR file.
stats     Get file statistics.
validate  Validate a STAR file.
"""
from typing import Any, Dict, List, Optional, TypeVar

_T = TypeVar("_T")

def read(path: str) -> Dict[str, Any]:
    """Read a STAR file.

    Parameters
    ----------
    path : str
        Path to the ``.star`` file.

    Returns
    -------
    dict
        ``{block_name: DataFrame | dict}``.
        Loop blocks become ``polars.DataFrame`` (preferred) or
        ``pandas.DataFrame``, or fall back to plain ``{column: [values...]}``
        dicts when neither library is installed.
        Simple blocks are always ``{key: value}`` dicts.

    Raises
    ------
    FileNotFoundError
        The file does not exist.
    ValueError
        The file is malformed.

    Example
    -------
    >>> import emstar
    >>> data = emstar.read("particles.star")
    >>> list(data.keys())
    ['general', 'particles']
    """
    ...

def write(data: Dict[str, Any], path: str) -> None:
    """Write data to a STAR file.

    Accepts both plain dicts and DataFrames.
    Loop blocks can be ``{column: [values...]}`` dicts, ``pandas.DataFrame``,
    or ``polars.DataFrame``. Simple blocks must be ``{key: value}`` dicts.
    A single DataFrame can be passed directly as the root block.

    Parameters
    ----------
    data : dict or DataFrame
        Block data as returned by ``read()``, or a plain dict of blocks,
        or a single DataFrame.
    path : str
        Output ``.star`` file path.

    Raises
    ------
    ValueError
        Data format is invalid or the file cannot be written.

    Example
    -------
    >>> import emstar
    >>> emstar.write({"x": [1.0, 2.0]}, "out.star")
    """
    ...

def stats(path: str) -> Dict[str, int]:
    """Get file statistics.

    Parameters
    ----------
    path : str
        Path to the ``.star`` file.

    Returns
    -------
    dict
        Keys:
        - ``n_blocks``: total number of data blocks
        - ``n_simple``: number of SimpleBlocks (key-value)
        - ``n_loop``: number of LoopBlocks (tabular)
        - ``total_loop_rows``: sum of rows across all loop blocks
        - ``total_simple_entries``: sum of entries across all simple blocks

    Raises
    ------
    FileNotFoundError
        The file does not exist.

    Example
    -------
    >>> import emstar
    >>> s = emstar.stats("particles.star")
    >>> s["n_blocks"]
    2
    """
    ...

def validate(path: str) -> None:
    """Validate a STAR file.

    Parameters
    ----------
    path : str
        Path to the ``.star`` file.

    Raises
    ------
    FileNotFoundError
        The file does not exist.
    ValueError
        The file is malformed (parse error).

    Example
    -------
    >>> import emstar
    >>> emstar.validate("particles.star")
    """
    ...
