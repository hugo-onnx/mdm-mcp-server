#!/usr/bin/env python3
# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Manual testing script for data export tools.

This script allows you to test the export functionality against a real IBM MDM instance.

Usage:
    1. Ensure your .env file is configured with MDM connection details
    2. Run: python test_export_manual.py

Environment variables required:
    - API_CLOUD_BASE_URL or API_CPD_BASE_URL
    - API_CLOUD_CRN or equivalent for CPD
    - Authentication credentials (API key or username/password)
"""

import os
import sys
import time
import logging
from unittest.mock import Mock

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from dotenv import load_dotenv
load_dotenv()

from data_ms.data_exports.service import DataExportService
from data_ms.data_exports.tool_models import (
    CreateDataExportRequest,
    DownloadDataExportRequest
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


def create_mock_context(session_id: str = "manual-test-session"):
    """Create a mock MCP context for testing."""
    ctx = Mock()
    ctx.session_id = session_id
    return ctx


def test_create_export_basic():
    """Test basic export creation."""
    logger.info("=" * 60)
    logger.info("TEST: Basic Export Creation")
    logger.info("=" * 60)
    
    service = DataExportService()
    ctx = create_mock_context()
    
    result = service.create_export(
        ctx=ctx,
        export_name="manual_test_export",
        export_type="entity",
        record_type="person",  # Change this to match your data model
        file_format="csv",
        compression_type="none"
    )
    
    if "error" in result:
        logger.error(f"Export creation failed: {result}")
        return None
    
    export_id = result.get('job_id') or result.get('id')
    logger.info(f"Export created successfully!")
    logger.info(f"  Export ID: {export_id}")
    logger.info(f"  Job Name: {result.get('job_name')}")
    logger.info(f"  Status: {result.get('status')}")
    logger.info(f"  File Name: {result.get('file_name')}")
    
    return export_id


def test_create_export_with_filter():
    """Test export creation with search criteria filter."""
    logger.info("=" * 60)
    logger.info("TEST: Export Creation with Filter")
    logger.info("=" * 60)
    
    service = DataExportService()
    ctx = create_mock_context()
    
    result = service.create_export(
        ctx=ctx,
        export_name="filtered_test_export",
        export_type="entity",
        record_type="person",  # Change this to match your data model
        file_format="csv",
        compression_type="zip",
        search_criteria={
            "query": {
                "expressions": [
                    {
                        "property": "*",  # Full-text search
                        "condition": "contains",
                        "value": "test"
                    }
                ]
            }
        }
    )
    
    if "error" in result:
        logger.error(f"Export creation failed: {result}")
        return None
    
    export_id = result.get('job_id') or result.get('id')
    logger.info(f"Filtered export created successfully!")
    logger.info(f"  Export ID: {export_id}")
    logger.info(f"  Status: {result.get('status')}")
    
    return export_id


def test_download_export(export_id: str):
    """Test downloading an export."""
    logger.info("=" * 60)
    logger.info(f"TEST: Download Export (ID: {export_id})")
    logger.info("=" * 60)
    
    service = DataExportService()
    ctx = create_mock_context()
    
    result = service.download_export(
        ctx=ctx,
        export_id=export_id
    )
    
    if "error" in result:
        logger.error(f"Download failed: {result}")
        return
    
    logger.info(f"Download completed successfully!")
    logger.info(f"  Export ID: {result.get('export_id')}")
    logger.info(f"  File Name: {result.get('file_name')}")
    logger.info(f"  Content Type: {result.get('content_type')}")
    logger.info(f"  File Size: {result.get('file_size')} bytes")
    logger.info(f"  Status: {result.get('status')}")
    
    if result.get('file_path'):
        logger.info(f"  Saved to: {result.get('file_path')}")


def test_get_export(export_id: str):
    """Test getting export job status."""
    logger.info("=" * 60)
    logger.info(f"TEST: Get Export Status (ID: {export_id})")
    logger.info("=" * 60)
    
    service = DataExportService()
    ctx = create_mock_context()
    
    result = service.get_export(
        ctx=ctx,
        export_id=export_id
    )
    
    if "error" in result:
        logger.error(f"Get export failed: {result}")
        return None
    
    logger.info(f"Export status retrieved successfully!")
    logger.info(f"  Job ID: {result.get('job_id')}")
    logger.info(f"  Status: {result.get('status')}")
    logger.info(f"  Export Type: {result.get('export_type')}")
    logger.info(f"  File Name: {result.get('file_name')}")
    logger.info(f"  File Expired: {result.get('file_expired')}")
    logger.info(f"  Start Time: {result.get('start_time')}")
    logger.info(f"  End Time: {result.get('end_time')}")
    
    return result.get('status')


def poll_export_status(export_id: str, max_wait_seconds: int = 120, poll_interval: int = 5):
    """Poll export status until complete or timeout."""
    service = DataExportService()
    ctx = create_mock_context()
    
    elapsed = 0
    while elapsed < max_wait_seconds:
        result = service.get_export(ctx=ctx, export_id=export_id)
        
        if "error" in result:
            logger.error(f"Error getting status: {result}")
            return None
        
        status = result.get('status')
        logger.info(f"  Status: {status} (elapsed: {elapsed}s)")
        
        if status == 'succeeded':
            logger.info("  Export completed successfully!")
            return status
        elif status in ('failed', 'canceled'):
            logger.error(f"  Export {status}!")
            return status
        
        time.sleep(poll_interval)
        elapsed += poll_interval
    
    logger.warning(f"  Timeout after {max_wait_seconds}s. Status: {status}")
    return status


def test_full_workflow():
    """Test the complete export workflow: create -> poll status -> download."""
    logger.info("=" * 60)
    logger.info("TEST: Full Export Workflow")
    logger.info("=" * 60)
    
    # Step 1: Create export
    logger.info("\nStep 1: Creating export...")
    export_id = test_create_export_basic()
    
    if not export_id:
        logger.error("Failed to create export. Aborting workflow.")
        return
    
    # Step 2: Poll for export to complete
    logger.info("\nStep 2: Polling export status...")
    status = poll_export_status(export_id, max_wait_seconds=120, poll_interval=5)
    
    if status != 'succeeded':
        logger.error(f"Export did not complete successfully. Status: {status}")
        return
    
    # Step 3: Download
    logger.info("\nStep 3: Downloading export file...")
    test_download_export(export_id)


def run_all_tests():
    """Run all manual tests."""
    logger.info("\n" + "=" * 60)
    logger.info("STARTING MANUAL EXPORT TESTS")
    logger.info("=" * 60 + "\n")
    
    # Check configuration
    from config import Config
    logger.info(f"Target Platform: {Config.M360_TARGET_PLATFORM}")
    logger.info(f"API Base URL: {Config.API_BASE_URL}")
    
    if not Config.API_BASE_URL:
        logger.error("API_BASE_URL is not configured. Please check your .env file.")
        return
    
    print("\nSelect a test to run:")
    print("1. Basic export creation")
    print("2. Export with search filter")
    print("3. Get export status (requires export ID)")
    print("4. Download export (requires export ID)")
    print("5. Full workflow (create -> poll status -> download)")
    print("6. Run all tests")
    print("0. Exit")
    
    choice = input("\nEnter your choice: ").strip()
    
    if choice == "1":
        test_create_export_basic()
    elif choice == "2":
        test_create_export_with_filter()
    elif choice == "3":
        export_id = input("Enter export ID: ").strip()
        test_get_export(export_id)
    elif choice == "4":
        export_id = input("Enter export ID: ").strip()
        test_download_export(export_id)
    elif choice == "5":
        test_full_workflow()
    elif choice == "6":
        test_create_export_basic()
        print()
        test_create_export_with_filter()
    elif choice == "0":
        print("Exiting.")
    else:
        print("Invalid choice.")


if __name__ == "__main__":
    run_all_tests()