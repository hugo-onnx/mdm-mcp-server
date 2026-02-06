# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Data export tools for IBM MDM MCP server.

HARDCODED VERSION: Simplified for entity exports.
"""

import logging
from typing import Optional

from fastmcp import Context
from .service import DataExportService
from .tool_models import (
    CreateDataExportRequest,
    GetDataExportRequest,
    DownloadDataExportRequest,
    ExportJobResponse,
    GetDataExportResponse,
    DownloadDataExportResponse,
    DataExportErrorResponse,
    CreateDataExportResponse,
    GetDataExportStatusResponse,
    DataExportDownloadResponse
)

logger = logging.getLogger(__name__)

_export_service: Optional[DataExportService] = None


def get_export_service() -> DataExportService:
    """Get or create the data export service instance."""
    global _export_service
    if _export_service is None:
        _export_service = DataExportService()
    return _export_service


def create_data_export(
    ctx: Context,
    request: CreateDataExportRequest
) -> CreateDataExportResponse:
    """
    Creates a new data export job in IBM MDM.
    
    HARDCODED for entity exports. The search_criteria is built internally
    with the structure that works with the MDM API.
    
    Args:
        ctx: MCP Context object (automatically injected)
        request: CreateDataExportRequest containing:
            - entity_type: Entity type to export (default: "creditentity")
            - file_format: "csv" (default), "tsv", "psv", or "json"
            - compression_type: "zip" (default), "tar", or "tgz"
            - crn: Cloud Resource Name (optional)
    
    Returns:
        ExportJobResponse with job_id and status, or DataExportErrorResponse on error.
    
    Example:
        create_data_export(
            request=CreateDataExportRequest(
                entity_type="creditentity"
            )
        )
    """
    service = get_export_service()
    
    result = service.create_export(
        ctx=ctx,
        entity_type=request.entity_type,
        file_format=request.file_format,
        compression_type=request.compression_type,
        crn=request.crn
    )
    
    if "error" in result:
        return DataExportErrorResponse(**result)
    else:
        return ExportJobResponse(**result)


def get_data_export(
    ctx: Context,
    request: GetDataExportRequest
) -> GetDataExportStatusResponse:
    """
    Gets information about a data export job.
    
    Returns success only if the export has completed successfully.
    
    Args:
        ctx: MCP Context object (automatically injected)
        request: GetDataExportRequest containing:
            - export_id: The unique identifier of the export job (required)
            - crn: Cloud Resource Name (optional)
    
    Returns:
        GetDataExportResponse if succeeded, or DataExportErrorResponse if still running/failed.
    """
    service = get_export_service()
    
    result = service.get_export(
        ctx=ctx,
        export_id=request.export_id,
        crn=request.crn
    )
    
    if "error" in result:
        return DataExportErrorResponse(**result)
    
    status = result.get("status", "unknown")
    
    if status == "succeeded":
        return GetDataExportResponse(**result)
    
    if status in ("failed", "canceled"):
        return DataExportErrorResponse(
            error="ExportFailed",
            status_code=400,
            message=f"Export job {request.export_id} {status}.",
            details={
                "export_id": request.export_id,
                "status": status,
                "job_id": result.get("job_id")
            }
        )
    
    # Still running
    return DataExportErrorResponse(
        error="ExportNotReady",
        status_code=202,
        message=f"Export job {request.export_id} is not ready. Status: {status}. Please wait and retry.",
        details={
            "export_id": request.export_id,
            "status": status,
            "job_id": result.get("job_id")
        }
    )


def download_data_export(
    ctx: Context,
    request: DownloadDataExportRequest
) -> DataExportDownloadResponse:
    """
    Downloads a completed data export file as base64.
    
    Args:
        ctx: MCP Context object (automatically injected)
        request: DownloadDataExportRequest containing:
            - export_id: The unique identifier of the export job (required)
            - crn: Cloud Resource Name (optional)
    
    Returns:
        DownloadDataExportResponse with file_content_base64, or DataExportErrorResponse on error.
    """
    service = get_export_service()
    
    result = service.download_export(
        ctx=ctx,
        export_id=request.export_id,
        crn=request.crn,
        include_base64=True
    )
    
    if "error" in result:
        return DataExportErrorResponse(**result)
    else:
        return DownloadDataExportResponse(**result)