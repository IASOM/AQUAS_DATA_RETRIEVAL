# Quick Start Guide

Get the project up and running in 5 minutes.

## For First-Time Users

### 1. Initial Setup (One Time)

```bash
# Clone or navigate to project
cd GIT

# Create virtual environment
python -m venv venv

# Activate virtual environment
# Windows:
venv\Scripts\activate
# Linux/Mac:
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Create data directories
python setup.py

# Copy and configure environment
copy .env.example .env
# Edit .env with your database credentials
```

### 2. Validate Setup

```bash
python validate_project.py
```

Should see:
```
✓ ALL CHECKS PASSED - Project is ready!
```

### 3. Run Your First Pipeline

```bash
# Run both pipelines
python run_pipeline.py

# Or specific pipeline
python run_pipeline.py --demand
python run_pipeline.py --diagnosis

# View available options
python run_pipeline.py --help
```

## For Returning Users

### Activate Environment

```bash
# Windows
venv\Scripts\activate

# Linux/Mac
source venv/bin/activate
```

### Run Pipeline

```bash
python run_pipeline.py --both
```

### Check Status

```bash
# Validate project state
python validate_project.py

# View latest logs (if implemented)
tail -f logs/*.log
```

## Project Files Overview

| File | Purpose |
|------|---------|
| `run_pipeline.py` | Main entry point - run pipelines here |
| `config/config.py` | All configuration settings |
| `.env` | Database credentials and paths |
| `requirements.txt` | Python dependencies |
| `pipelines/demand/` | Demand pipeline code |
| `pipelines/diagnosis/` | Diagnosis pipeline code |
| `pipelines/shared/` | Shared utilities and DB code |
| `data/` | Pipeline data and state files |

## Common Commands

```bash
# Run demand pipeline only
python run_pipeline.py --demand

# Run diagnosis pipeline only  
python run_pipeline.py --diagnosis

# Run both pipelines
python run_pipeline.py --both

# View help
python run_pipeline.py --help

# Validate configuration
python validate_project.py

# Reset pipeline state (process all data again)
rm data/demand_pipeline/state/state.json
rm data/diagnosis_pipeline/state/state.json
python run_pipeline.py --both
```

## Configuration

### Database Settings (.env)

```env
DB_SERVER=your-synapse-server.sql.azuresynapse.net
DB_DATABASE=your_database_name
AUTH_MODE=ActiveDirectoryIntegrated
```

### File Paths (.env)

```env
BASE_DIR=C:/path/to/GIT
UP_RS_FILE=C:/path/to/UPperRS.xlsx
```

### Logging Level (.env)

```env
LOG_LEVEL=INFO    # INFO, DEBUG, WARNING, ERROR, CRITICAL
```

## Troubleshooting

### Issue: "ModuleNotFoundError"

```bash
# Activate virtual environment
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt
```

### Issue: Database Connection Error

```bash
# Check .env configuration
cat .env

# Verify server and database name
# Test connection manually if needed

# Check ODBC Driver
odbcconf /s
```

### Issue: File Not Found

```bash
# Create missing directories
python setup.py

# Verify paths in .env are correct
cat .env
```

## File Locations

### Output Files

- **Demand incremental**: `data/demand_pipeline/incremental/demanda_*.csv`
- **Demand final**: `data/demand_pipeline/finals/demanda_*.csv`
- **Diagnosis incremental**: `data/diagnosis_pipeline/incremental/selected_*.csv`
- **Diagnosis final**: `data/diagnosis_pipeline/finals/selected_*.csv`

### State Files

- **Demand state**: `data/demand_pipeline/state/state.json`
- **Diagnosis state**: `data/diagnosis_pipeline/state/state.json`

### Logs

- Check terminal output for detailed logs

## Next Steps

1. ✓ Setup complete
2. ✓ Tested and validated
3. Now:
   - Read [README.md](README.md) for detailed documentation
   - Check [MIGRATION.md](MIGRATION.md) if coming from old structure
   - Review [PROJECT_STRUCTURE.md](PROJECT_STRUCTURE.md) for architecture

## Getting Help

| Problem | Solution |
|---------|----------|
| Import errors | Run `python validate_project.py` |
| Database errors | Check `.env` file |
| Missing files | Run `python setup.py` |
| Configuration issues | Review `.env.example` |
| Detailed info | See [README.md](README.md) |

---

**Tip**: Bookmark [README.md](README.md) for comprehensive documentation!
