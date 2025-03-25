import subprocess
import sys

def test_pip_install():
    """Test if the package installs correctly using pip."""
    
    # Run `pip install .` in a subprocess
    result = subprocess.run([sys.executable, "-m", "pip", "install", "."], capture_output=True, text=True)
    
    assert result.returncode == 0, f"pip install failed:\n{result.stderr}"

def test_import_duckdb_tools():
    """Test if duckdb_tools can be imported after installation."""
    
    result = subprocess.run([sys.executable, "-c", "import duckdb_tools; print(duckdb_tools)"], capture_output=True, text=True)

    assert result.returncode == 0, f"Import failed:\n{result.stderr}"
