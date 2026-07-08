"""Tests for the emstar Python bindings.

Run with::

    pip install maturin
    maturin build --features python
    pip install target/wheels/emstar-*.whl
    python tests/test_python.py
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

DATA_DIR = os.path.join(os.path.dirname(__file__), "data")
EXAMPLE = """data_general
_rlnImageSize 256
_rlnPixelSize 1.06

data_particles
loop_
_rlnCoordinateX #1
_rlnCoordinateY #2
91.8  83.6
97.6  80.4
"""


def test_import():
    import emstar
    assert hasattr(emstar, "read")
    assert hasattr(emstar, "write")
    assert hasattr(emstar, "stats")
    assert hasattr(emstar, "validate")


def test_read_write_roundtrip():
    import emstar

    with tempfile.NamedTemporaryFile(suffix=".star", mode="w", delete=False) as f:
        f.write(EXAMPLE)
        f.flush()
        path = f.name

    # Read
    data = emstar.read(path)
    assert isinstance(data, dict)
    assert "general" in data
    assert "particles" in data

    # Simple block
    general = data["general"]
    assert isinstance(general, dict)
    assert general.get("_rlnImageSize") == 256

    # Loop block
    particles = data["particles"]
    if hasattr(particles, "columns"):
        # DataFrame mode (polars/pandas installed)
        cols = list(particles.columns)
    else:
        # Dict mode (no polars/pandas)
        cols = list(particles.keys())
    assert "rlnCoordinateX" in cols
    assert "rlnCoordinateY" in cols

    # Write roundtrip
    out = path + ".out.star"
    emstar.write(data, out)
    data2 = emstar.read(out)
    assert len(data2) == len(data)
    os.unlink(out)
    os.unlink(path)


def test_stats():
    import emstar

    with tempfile.NamedTemporaryFile(suffix=".star", mode="w", delete=False) as f:
        f.write(EXAMPLE)
        f.flush()
        path = f.name

    s = emstar.stats(path)
    assert isinstance(s, dict)
    assert s["n_blocks"] == 2
    assert s["n_simple"] == 1
    assert s["n_loop"] == 1
    assert s["total_loop_rows"] == 2
    assert s["total_simple_entries"] == 2
    os.unlink(path)


def test_validate():
    import emstar

    with tempfile.NamedTemporaryFile(suffix=".star", mode="w", delete=False) as f:
        f.write(EXAMPLE)
        f.flush()
        path = f.name

    # Valid file
    emstar.validate(path)  # should not raise

    # Invalid file
    invalid = path + ".invalid"
    with open(invalid, "w") as f:
        f.write("not a star file")
    try:
        emstar.validate(invalid)
        assert False, "should have raised"
    except ValueError:
        pass
    os.unlink(path)
    os.unlink(invalid)


def test_real_data_files():
    """Read all real RELION STAR files from tests/data."""
    import emstar

    if not os.path.isdir(DATA_DIR):
        print(f"  SKIP: {DATA_DIR} not found")
        return

    star_files = []
    for root, dirs, files in os.walk(DATA_DIR):
        for f in files:
            if f.endswith(".star"):
                star_files.append(os.path.join(root, f))

    assert len(star_files) > 0, f"No .star files found in {DATA_DIR}"
    print(f"  Testing {len(star_files)} real files...")

    for path in star_files:
        try:
            data = emstar.read(path)
            assert isinstance(data, dict)
            assert len(data) > 0
        except Exception as e:
            print(f"  FAIL: {path}: {e}")
            raise

    print(f"  All {len(star_files)} files OK")


def test_write_from_dict():
    import emstar

    data = {
        "simple": {"key": 42},
        "loop": {"x": [1.0, 2.0], "y": [3.0, 4.0]},
    }
    with tempfile.NamedTemporaryFile(suffix=".star", mode="w", delete=False) as f:
        path = f.name

    emstar.write(data, path)
    read_back = emstar.read(path)
    assert len(read_back) == 2
    assert "simple" in read_back
    assert "loop" in read_back
    os.unlink(path)


def test_simple_block_values():
    import emstar

    star = "data_test\n_k_int 42\n_k_float 3.14\n_k_str hello\n_k_true yes\n_k_null <NA>\n"
    with tempfile.NamedTemporaryFile(suffix=".star", mode="w", delete=False) as f:
        f.write(star)
        f.flush()
        path = f.name

    data = emstar.read(path)
    block = data["test"]
    assert block["k_int"] == 42
    assert block["k_float"] == 3.14
    assert block["k_str"] == "hello"
    assert block["k_true"] is True
    assert block["k_null"] is None
    os.unlink(path)


if __name__ == "__main__":
    try:
        import emstar
    except ImportError:
        print(" ✗ emstar is not installed. Build it first:")
        print("   pip install maturin")
        print("   maturin build --features python")
        print("   pip install target/wheels/emstar-*.whl")
        sys.exit(1)

    tests = [
        test_import,
        test_read_write_roundtrip,
        test_stats,
        test_validate,
        test_write_from_dict,
        test_simple_block_values,
        test_real_data_files,
    ]
    passed = 0
    failed = 0
    for t in tests:
        try:
            t()
            print(f"  ✓ {t.__name__}")
            passed += 1
        except Exception as e:
            print(f"  ✗ {t.__name__}: {e}")
            failed += 1
    print(f"\n  {passed} passed, {failed} failed")
    sys.exit(1 if failed else 0)
