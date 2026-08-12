"""Validation script - Check configuration and environment."""
import sys
from pathlib import Path
import os

# Add project root to path
PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(PROJECT_ROOT))

def validate_environment():
    """Validate project configuration and environment."""
    
    print("\n" + "="*80)
    print("PROJECT VALIDATION")
    print("="*80 + "\n")
    
    checks = []
    
    # 1. Check Python version
    print("1. Checking Python version...")
    if sys.version_info >= (3, 9):
        print(f"   ✓ Python {sys.version.split()[0]} (OK)")
        checks.append(True)
    else:
        print(f"   ✗ Python {sys.version.split()[0]} (requires 3.9+)")
        checks.append(False)
    
    # 2. Check directory structure
    print("\n2. Checking directory structure...")
    required_dirs = [
        "config",
        "pipelines",
        "pipelines/shared",
        "pipelines/demand",
        "pipelines/diagnosis",
        "data",
    ]
    
    dir_ok = True
    for dir_path in required_dirs:
        full_path = PROJECT_ROOT / dir_path
        if full_path.exists():
            print(f"   ✓ {dir_path}")
        else:
            print(f"   ✗ {dir_path} (missing)")
            dir_ok = False
    checks.append(dir_ok)
    
    # 3. Check key files
    print("\n3. Checking key files...")
    required_files = [
        "config/config.py",
        "pipelines/shared/__init__.py",
        "pipelines/shared/db.py",
        "pipelines/shared/utils.py",
        "requirements.txt",
        ".env.example",
        "run_pipeline.py",
    ]
    
    files_ok = True
    for file_path in required_files:
        full_path = PROJECT_ROOT / file_path
        if full_path.exists():
            print(f"   ✓ {file_path}")
        else:
            print(f"   ✗ {file_path} (missing)")
            files_ok = False
    checks.append(files_ok)
    
    # 4. Check .env configuration
    print("\n4. Checking environment configuration...")
    env_file = PROJECT_ROOT / ".env"
    if env_file.exists():
        print(f"   ✓ .env file found")
        # Check if it has been edited from template
        with open(env_file) as f:
            content = f.read()
            if "YourUsername" in content or "your-server" in content.lower():
                print("   ⚠ .env may not be configured (contains placeholder values)")
                checks.append(False)
            else:
                print("   ✓ .env appears to be configured")
                checks.append(True)
    else:
        print(f"   ✗ .env file not found")
        print("      Copy .env.example to .env and update with your settings")
        checks.append(False)
    
    # 5. Check dependencies
    print("\n5. Checking Python dependencies...")
    try:
        import pandas
        print(f"   ✓ pandas {pandas.__version__}")
    except ImportError:
        print("   ✗ pandas not installed")
        checks.append(False)
        return
    
    try:
        import pyodbc
        pyodbc_version = getattr(pyodbc, "version", None) or getattr(
            pyodbc, "__version__", "installed"
        )
        print(f"   ✓ pyodbc {pyodbc_version}")
    except ImportError:
        print("   ✗ pyodbc not installed")
        checks.append(False)
    
    try:
        import sqlalchemy
        print(f"   ✓ sqlalchemy {sqlalchemy.__version__}")
    except ImportError:
        print("   ⚠ sqlalchemy not installed (optional)")
    
    # 6. Test imports
    print("\n6. Testing core imports...")
    try:
        from config.config import get_config, DemandConfig, DiagnosisConfig
        print("   ✓ config module")
        
        from pipelines.shared import get_connection, load_state, save_state
        print("   ✓ shared utilities")
        
        checks.append(True)
    except Exception as e:
        print(f"   ✗ Import error: {e}")
        checks.append(False)
    
    # Summary
    print("\n" + "="*80)
    if all(checks):
        print("✓ ALL CHECKS PASSED - Project is ready!")
        print("\nYou can now run:")
        print("  python run_pipeline.py --demand")
        print("  python run_pipeline.py --diagnosis")
        print("  python run_pipeline.py --both")
    else:
        print("✗ SOME CHECKS FAILED - Please fix the issues above")
        print("\nCommon fixes:")
        print("  1. Create data directories: python setup.py")
        print("  2. Configure .env: copy .env.example .env (then edit)")
        print("  3. Install dependencies: pip install -r requirements.txt")
    print("="*80 + "\n")
    
    return all(checks)

if __name__ == "__main__":
    success = validate_environment()
    sys.exit(0 if success else 1)
