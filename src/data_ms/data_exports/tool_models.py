# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Pydantic models for data export tool interface.

HARDCODED VERSION: Simplified for entity exports with creditentity.
"""

from typing import Optional, List, Dict, Any, Union
from pydantic import BaseModel, Field


# =============================================================================
# Request Models
# =============================================================================

class CreateDataExportRequest(BaseModel):
    """
    Request model for create_data_export tool.
    
    HARDCODED for entity exports - search_criteria is built internally.
    file_name is optional in the API, so we omit it entirely.
    """
    
    entity_type: str = Field(
        default="creditentity",
        description="Entity type to export (e.g., 'creditentity')"
    )
    
    file_format: str = Field(
        default="csv",
        description="Format of the export file: csv, tsv, psv, or json"
    )
    
    compression_type: str = Field(
        default="zip",
        description="Compression type: zip, tar, or tgz"
    )
    
    crn: Optional[str] = Field(
        None,
        description="Cloud Resource Name identifying the tenant"
    )


class GetDataExportRequest(BaseModel):
    """Request model for get_data_export tool."""
    
    export_id: str = Field(
        ...,
        min_length=1,
        description="The unique identifier of the export job"
    )
    
    crn: Optional[str] = Field(
        None,
        description="Cloud Resource Name identifying the tenant"
    )


class DownloadDataExportRequest(BaseModel):
    """Request model for download_data_export tool."""
    
    export_id: str = Field(
        ...,
        description="The unique identifier of the export job to download"
    )
    
    crn: Optional[str] = Field(
        None,
        description="Cloud Resource Name identifying the tenant"
    )


# =============================================================================
# Response Models
# =============================================================================

class ExportJobResponse(BaseModel):
    """Response model for export job creation."""
    
    job_id: Optional[str] = Field(None)
    id: Optional[str] = Field(None)
    job_name: Optional[str] = Field(None)
    job_type: Optional[str] = Field(None)
    status: Optional[str] = Field(None)
    export_type: Optional[str] = Field(None)
    file_name: Optional[str] = Field(None)
    file_expired: Optional[bool] = Field(None)
    start_time: Optional[str] = Field(None)
    end_time: Optional[str] = Field(None)
    process_ids: Optional[List[str]] = Field(None)
    search_criteria: Optional[Dict[str, Any]] = Field(None)
    additional_info: Optional[Dict[str, Any]] = Field(None)
    
    @property
    def export_id(self) -> Optional[str]:
        return self.job_id or self.id


class GetDataExportResponse(BaseModel):
    """Response model for get_data_export operations."""
    
    job_id: str
    job_type: str
    status: str
    export_type: str
    file_name: str
    file_expired: bool
    search_criteria: Optional[Dict[str, Any]] = Field(None)
    start_time: Optional[str] = Field(None)
    end_time: Optional[str] = Field(None)
    process_ids: Optional[List[str]] = Field(None)
    additional_info: Optional[Dict[str, Any]] = Field(None)
    record_count: Optional[int] = Field(None)


class DownloadDataExportResponse(BaseModel):
    """Response model for download_data_export operations."""
    
    export_id: str
    file_name: Optional[str] = Field(None)
    content_type: Optional[str] = Field(None)
    file_size: Optional[int] = Field(None)
    file_content_base64: Optional[str] = Field(None)
    file_path: Optional[str] = Field(None)
    status: Optional[str] = Field(None)
    message: Optional[str] = Field(None)


class DataExportErrorResponse(BaseModel):
    """Error response model for data export operations."""
    
    error: str
    status_code: int
    message: str
    details: Optional[Dict[str, Any]] = Field(None)


# =============================================================================
# Type Aliases
# =============================================================================

CreateDataExportResponse = Union[ExportJobResponse, DataExportErrorResponse]
GetDataExportStatusResponse = Union[GetDataExportResponse, DataExportErrorResponse]
DataExportDownloadResponse = Union[DownloadDataExportResponse, DataExportErrorResponse]