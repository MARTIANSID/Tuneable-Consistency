#!/bin/bash

# Setup script for Online RL Budget Optimization with gRPC

set -e

echo "=========================================="
echo "  RL Budget Optimization Setup (gRPC)"
echo "=========================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Check Python
echo "Step 1: Checking Python..."
if command -v python3 &> /dev/null; then
    PYTHON_VERSION=$(python3 --version)
    echo -e "${GREEN}✓${NC} $PYTHON_VERSION"
else
    echo -e "${RED}✗ Python 3 not found${NC}"
    exit 1
fi

# Step 2: Create virtual environment
echo ""
echo "Step 2: Setting up Python environment..."
cd analysis/models

if [ ! -d "venv" ]; then
    python3 -m venv venv
    echo -e "${GREEN}✓${NC} Virtual environment created"
else
    echo -e "${GREEN}✓${NC} Virtual environment already exists"
fi

# Activate venv
source venv/bin/activate

# Step 3: Install Python dependencies
echo ""
echo "Step 3: Installing Python dependencies..."
pip install --upgrade pip > /dev/null
pip install -r requirements.txt
echo -e "${GREEN}✓${NC} Python dependencies installed"

# Step 4: Generate Python gRPC code
echo ""
echo "Step 4: Generating Python gRPC code..."
python -m grpc_tools.protoc \
    -I../../src/main/resources \
    --python_out=. \
    --grpc_python_out=. \
    ../../src/main/resources/RLBudget.proto

if [ -f "rl_budget_pb2.py" ]; then
    echo -e "${GREEN}✓${NC} Python gRPC code generated"
else
    echo -e "${RED}✗ Failed to generate Python gRPC code${NC}"
    exit 1
fi

# Step 5: Create checkpoint directory
echo ""
echo "Step 5: Creating directories..."
mkdir -p checkpoints logs export
echo -e "${GREEN}✓${NC} Directories created"

# Step 6: Check Java/Maven
echo ""
echo "Step 6: Checking Java and Maven..."
cd ../..

if command -v mvn &> /dev/null; then
    echo -e "${GREEN}✓${NC} Maven found"
else
    echo -e "${RED}✗ Maven not found${NC}"
    exit 1
fi

# Step 7: Compile Java and generate gRPC code
echo ""
echo "Step 7: Compiling Java and generating gRPC code..."
mvn clean compile
echo -e "${GREEN}✓${NC} Java compilation complete"

# Summary
echo ""
echo "=========================================="
echo "  Setup Complete!"
echo "=========================================="
echo ""
echo "Next steps:"
echo ""
echo "1. Start the gRPC server (Python):"
echo "   cd analysis/models"
echo "   source venv/bin/activate"
echo "   python grpc_server.py"
echo ""
echo "2. In your Java code, use RLModelGrpcClient:"
echo "   RLModelGrpcClient client = new RLModelGrpcClient(\"localhost\", 50051);"
echo "   double budget = client.predictBudgetAndRecord(...);"
echo ""
echo "3. The model will train automatically every 2 minutes!"
echo ""
echo "For more details, see ONLINE_TRAINING_GUIDE.md"
echo ""
