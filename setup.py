"""Setup script for Windows - Creates directory structure and initializes project."""
import os
import shutil
from pathlib import Path

def setup_project():
    """Initialize project structure and directories."""
    
    project_root = Path(__file__).parent
    print(f"Setting up project at: {project_root}")
    
    # Create data directories
    data_dirs = [
        "data/demand_pipeline/state",
        "data/demand_pipeline/incremental",
        "data/demand_pipeline/finals",
        "data/diagnosis_pipeline/state",
        "data/diagnosis_pipeline/incremental",
        "data/diagnosis_pipeline/finals",
        "data/diagnosis_pipeline/selected_codes",
        "selections",
    ]
    
    for dir_path in data_dirs:
        full_path = project_root / dir_path
        full_path.mkdir(parents=True, exist_ok=True)
        print(f"✓ Created: {dir_path}")
    
    # Check for .env file
    env_file = project_root / ".env"
    if not env_file.exists():
        print("\n⚠ .env file not found!")
        print("  Copy .env.example to .env and update with your settings:")
        print(f"  copy {project_root / '.env.example'} {env_file}")
    else:
        print("✓ .env file found")
    
    print("\n✓ Project setup complete!")
    print("\nNext steps:")
    print("1. Activate virtual environment: venv\\Scripts\\activate")
    print("2. Install requirements: pip install -r requirements.txt")
    print("3. Configure .env with your database settings")
    print("4. Run: python run_pipeline.py --help")

if __name__ == "__main__":
    setup_project()
