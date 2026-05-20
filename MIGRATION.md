# Migration Guide: Old Structure → New Structure

This guide helps you migrate from the old project structure to the new, improved organization.

## Why Migrate?

The new structure provides:
- **Better organization**: Separation of code and data
- **Reduced duplication**: Single source of truth for utilities
- **Improved usability**: Single entry point for running pipelines
- **Better maintainability**: Clearer module structure
- **Centralized config**: All settings in one place

## Before and After

### Old Structure
```
src/
├── config.py                    (Hard-coded paths)
├── main.py                      (Not used)
├── aggregation.py               (Duplicated)
├── db.py                        (Duplicated)
├── incremental.py               (Duplicated)
├── transformations.py           (Duplicated)
├── utils.py                     (Duplicated)
├── demanda/
│   ├── config.py
│   ├── main.py
│   └── ...
└── diagnosis/
    ├── config.py
    ├── diagnosis_main.py
    └── ...
demand_pipeline/                 (Data mixed with code)
diagnosis_pipeline/              (Data mixed with code)
daily_run.py                     (Entry point)
```

### New Structure
```
config/
└── config.py                    (Centralized config)
pipelines/
├── shared/                      (Shared utilities - single copy)
│   ├── db.py
│   ├── utils.py
│   └── logging_config.py
├── demand/                      (Copy from src/demanda/)
│   ├── config.py
│   ├── main.py
│   └── ...
└── diagnosis/                   (Copy from src/diagnosis/)
    ├── config.py
    ├── diagnosis_main.py
    └── ...
data/                            (Data separated from code)
├── demand_pipeline/
└── diagnosis_pipeline/
run_pipeline.py                  (Single entry point)
```

## Step-by-Step Migration

### Phase 1: Preparation

1. **Backup Your Current Setup**
   ```bash
   # Create a backup of everything
   mkdir -p backup
   robocopy . backup /E /EXCLUDE:venv,.git
   ```

2. **Check Your Current Data**
   ```bash
   # Verify current data locations
   dir demand_pipeline
   dir diagnosis_pipeline
   ```

### Phase 2: Set Up New Structure

1. **Install Setup Script** (Already done by new structure)

2. **Create Data Directories**
   ```bash
   # Windows
   python setup.py
   # or
   setup.bat
   ```

   ```bash
   # Linux/Mac
   bash setup.sh
   ```

### Phase 3: Migrate Data

1. **Copy Demand Pipeline Data**
   ```bash
   # Windows
   robocopy demand_pipeline\state data\demand_pipeline\state /E
   robocopy demand_pipeline\incremental data\demand_pipeline\incremental /E
   robocopy demand_pipeline\finals data\demand_pipeline\finals /E
   ```

   ```bash
   # Linux/Mac
   cp -r demand_pipeline/state/* data/demand_pipeline/state/
   cp -r demand_pipeline/incremental/* data/demand_pipeline/incremental/
   cp -r demand_pipeline/finals/* data/demand_pipeline/finals/
   ```

2. **Copy Diagnosis Pipeline Data**
   ```bash
   # Windows
   robocopy diagnosis_pipeline\state data\diagnosis_pipeline\state /E
   robocopy diagnosis_pipeline\incremental data\diagnosis_pipeline\incremental /E
   robocopy diagnosis_pipeline\finals data\diagnosis_pipeline\finals /E
   robocopy diagnosis_pipeline\selected_codes data\diagnosis_pipeline\selected_codes /E
   ```

   ```bash
   # Linux/Mac
   cp -r diagnosis_pipeline/state/* data/diagnosis_pipeline/state/
   cp -r diagnosis_pipeline/incremental/* data/diagnosis_pipeline/incremental/
   cp -r diagnosis_pipeline/finals/* data/diagnosis_pipeline/finals/
   cp -r diagnosis_pipeline/selected_codes/* data/diagnosis_pipeline/selected_codes/
   ```

### Phase 4: Migrate Code

1. **Move Pipeline Modules**
   The pipeline code is already in `pipelines/` subdirectories.
   You may need to update imports if you have custom code.

2. **Update Imports in Pipeline Files**
   
   Change from:
   ```python
   from config import *
   from db import get_connection
   from utils import load_state
   ```
   
   To:
   ```python
   from pipelines.shared import get_connection, load_state
   from config.config import get_config, DemandConfig
   ```

3. **Update Configuration References**
   
   Old style (hard-coded):
   ```python
   BASE_DIR = Path("C:/Users/ghernandezgu/Desktop/PREDAP/demand_pipeline")
   ```
   
   New style (from config):
   ```python
   from config.config import DemandConfig
   BASE_DIR = DemandConfig.PIPELINE_DATA_DIR
   ```

### Phase 5: Configure Environment

1. **Create .env File**
   ```bash
   copy .env.example .env
   ```

2. **Edit .env**
   ```env
   # Database Configuration
   DB_SERVER=your-server.sql.azuresynapse.net
   DB_DATABASE=your_database
   
   # Base Paths
   BASE_DIR=C:/Users/YourUsername/Desktop/GIT
   UP_RS_FILE=C:/path/to/UPperRS.xlsx
   ```

### Phase 6: Test Migration

1. **Validate Project**
   ```bash
   python validate_project.py
   ```

2. **Test Demand Pipeline**
   ```bash
   python run_pipeline.py --demand
   ```

3. **Test Diagnosis Pipeline**
   ```bash
   python run_pipeline.py --diagnosis
   ```

4. **Test Both**
   ```bash
   python run_pipeline.py --both
   ```

### Phase 7: Cleanup (After Successful Testing)

1. **Verify New Setup Works**
   - Check that output files are created
   - Verify state files are updated
   - Check logs for errors

2. **Archive Old Structure**
   ```bash
   # Create archive
   mkdir -p archive
   move src archive/src_backup
   move demand_pipeline archive/
   move diagnosis_pipeline archive/
   ```

3. **Update Git**
   ```bash
   git add -A
   git commit -m "Refactor: Reorganize project structure

   - Moved code to pipelines/ directory
   - Moved data to data/ directory
   - Centralized configuration in config/
   - Created shared utilities module
   - Added comprehensive documentation
   - Unified entry point: run_pipeline.py"
   ```

## Troubleshooting Migration Issues

### Issue: Import Errors After Migration

**Solution**: Ensure all imports use the new paths
```python
# New correct imports
from pipelines.shared import get_connection, load_state, save_state
from config.config import get_config, DemandConfig, DiagnosisConfig
```

### Issue: File Not Found Errors

**Solution**: Verify data directories exist
```bash
python setup.py                # Create directories
python validate_project.py     # Check structure
```

### Issue: Database Connection Failed

**Solution**: Check .env configuration
```bash
# Verify settings in .env match your database
cat .env
# Update if needed
```

### Issue: State File Location Changed

**Solution**: Manually move state files
```bash
# Move old state to new location
copy demand_pipeline\state\state.json data\demand_pipeline\state\
copy diagnosis_pipeline\state\state.json data\diagnosis_pipeline\state\
```

## Rollback Plan

If you need to rollback to the old structure:

```bash
# Restore from backup
robocopy backup . /E /Y

# Or restore from git
git reset --hard <previous-commit>
git clean -fd
```

## Quick Comparison

| Task | Old | New |
|------|-----|-----|
| Run demand pipeline | `python src/demanda/main.py` | `python run_pipeline.py --demand` |
| Run both pipelines | Run each separately | `python run_pipeline.py --both` |
| Update config | Edit hardcoded paths | Edit `.env` file |
| Shared utilities | Duplicated in each module | One copy in `pipelines/shared/` |
| Entry point | Multiple scripts | Single `run_pipeline.py` |
| Data location | Mixed with code | Separate `data/` directory |

## After Migration

1. **Recommended**: Add pipeline scheduling
   ```bash
   # Windows Task Scheduler
   # Linux cron
   0 1 * * * python /path/to/GIT/run_pipeline.py --both
   ```

2. **Recommended**: Set up monitoring
   - Check log files for errors
   - Monitor state files for updates
   - Verify output files are generated

3. **Optional**: Add CI/CD
   - Set up automatic validation
   - Add tests
   - Automate deployments

## Need Help?

1. Check [README.md](README.md) for general information
2. Check [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for structure details
3. Run `python validate_project.py` to check configuration
4. Review log files in `data/{pipeline}_pipeline/state/`

---

**Migration Status**: Once complete, you can safely remove the old `src/`, `demand_pipeline/`, and `diagnosis_pipeline/` directories (keep backups first!).
