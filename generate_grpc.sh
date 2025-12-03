#!/bin/bash

# Quick script to generate gRPC code for both Python and Java

echo "Generating gRPC code from RLBudget.proto..."
echo ""

# Python code generation
echo "1. Generating Python gRPC code..."
cd analysis/models

if [ ! -d "venv" ]; then
    echo "Error: Python virtual environment not found. Run setup_grpc.sh first."
    exit 1
fi

source venv/bin/activate

python -m grpc_tools.protoc \
    -I../../src/main/resources \
    --python_out=. \
    --grpc_python_out=. \
    ../../src/main/resources/RLBudget.proto

if [ -f "rl_budget_pb2.py" ] && [ -f "rl_budget_pb2_grpc.py" ]; then
    echo "✓ Python gRPC code generated:"
    echo "  - rl_budget_pb2.py"
    echo "  - rl_budget_pb2_grpc.py"
else
    echo "✗ Failed to generate Python gRPC code"
    exit 1
fi

cd ../..

# Java code generation
echo ""
echo "2. Generating Java gRPC code..."
mvn compile > /dev/null 2>&1

if [ -d "target/generated-sources/protobuf/grpc-java/org/ds/rl" ]; then
    echo "✓ Java gRPC code generated in target/generated-sources/protobuf/"
else
    echo "✗ Failed to generate Java gRPC code"
    echo "   Try running: mvn clean compile"
    exit 1
fi

echo ""
echo "=========================================="
echo "✓ gRPC code generation complete!"
echo "=========================================="
echo ""
echo "You can now:"
echo "  1. Start the server: cd analysis/models && python grpc_server.py"
echo "  2. Run your Java application"
echo ""
