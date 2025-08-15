#!/bin/bash
set -euo pipefail

# Simple validation script for kumo-rfm-mcp wheels
echo "🔍 Validating wheels for release..."

# Check if dist directory exists
if [ ! -d "dist" ]; then
    echo "❌ Error: dist/ directory not found"
    exit 1
fi

# Check if any wheel files exist
wheel_files=(dist/*.whl)
if [ ! -e "${wheel_files[0]}" ]; then
    echo "❌ Error: No wheel files found in dist/"
    exit 1
fi

echo "📦 Found wheel files:"
ls -lah dist/*.whl

# Validate each wheel
for wheel in dist/*.whl; do
    echo "🔍 Validating $wheel..."

    # Check wheel file is not empty
    if [ ! -s "$wheel" ]; then
        echo "❌ Error: $wheel is empty"
        exit 1
    fi

    # Check wheel can be listed (basic integrity check)
    if ! python -m zipfile -l "$wheel" > /dev/null 2>&1; then
        echo "❌ Error: $wheel is corrupted or not a valid zip file"
        exit 1
    fi

    # Check wheel contains expected files
    if ! python -m zipfile -l "$wheel" | grep -q "kumo_rfm_mcp/server.py"; then
        echo "❌ Error: $wheel missing expected kumo_rfm_mcp/server.py"
        exit 1
    fi

    if ! python -m zipfile -l "$wheel" | grep -q "kumo_rfm_mcp-.*\.dist-info/METADATA"; then
        echo "❌ Error: $wheel missing METADATA file"
        exit 1
    fi

    # Check wheel has correct naming (should be universal wheel for pure Python)
    if [[ ! "$wheel" =~ kumo_rfm_mcp-.*-py3-none-any\.whl$ ]]; then
        echo "❌ Error: $wheel has unexpected naming. Expected: kumo_rfm_mcp-*-py3-none-any.whl"
        exit 1
    fi

    echo "✅ $wheel is valid"
done

# Check if source distribution exists
sdist_files=(dist/*.tar.gz)
if [ -e "${sdist_files[0]}" ]; then
    echo "📦 Found source distribution:"
    ls -lah dist/*.tar.gz

    for sdist in dist/*.tar.gz; do
        echo "🔍 Validating $sdist..."

        if [ ! -s "$sdist" ]; then
            echo "❌ Error: $sdist is empty"
            exit 1
        fi

        # Basic integrity check for tar.gz
        if ! tar -tzf "$sdist" > /dev/null 2>&1; then
            echo "❌ Error: $sdist is corrupted or not a valid tar.gz file"
            exit 1
        fi

        echo "✅ $sdist is valid"
    done
fi

echo "🎉 All wheel validation checks passed!"
