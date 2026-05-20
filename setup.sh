#!/bin/bash
# Setup script for Linux/Mac - Creates directory structure

echo "Setting up project directories..."

# Create data directory structure
mkdir -p data/demand_pipeline/state
mkdir -p data/demand_pipeline/incremental
mkdir -p data/demand_pipeline/finals
mkdir -p data/diagnosis_pipeline/state
mkdir -p data/diagnosis_pipeline/incremental
mkdir -p data/diagnosis_pipeline/finals
mkdir -p data/diagnosis_pipeline/selected_codes
mkdir -p selections

echo ""
echo "Project directories created successfully!"
echo ""
echo "Next steps:"
echo "1. Create virtual environment: python3 -m venv venv"
echo "2. Activate virtual environment: source venv/bin/activate"
echo "3. Install requirements: pip install -r requirements.txt"
echo "4. Copy and configure: cp .env.example .env"
echo "5. Edit .env with your database settings"
echo ""
echo "Then run: python run_pipeline.py --help"
