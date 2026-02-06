# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Pydantic models for data export domain objects.

HARDCODED VERSION: Minimal models - search criteria is hardcoded in service layer.
"""

from typing import List, Dict, Any, Optional, Literal
from pydantic import BaseModel, Field


class ExportJob(BaseModel):
    """Represents an export job with its configuration and status."""
    
    id: str = Field(..., description="Unique identifier for the export job")
    job_name: Optional[str] = Field(None, description="User-provided name")
    status: Optional[str] = Field(None, description="Current status")
    export_type: Optional[str] = Field(None, description="Type of data being exported")
    file_name: Optional[str] = Field(None, description="Name of the generated file")
    file_expired: Optional[bool] = Field(None, description="Whether file has expired")
    start_time: Optional[str] = Field(None, description="Job start timestamp")
    end_time: Optional[str] = Field(None, description="Job completion timestamp")


class ExportJobList(BaseModel):
    """A list of export jobs."""
    
    exports: List[ExportJob] = Field(default_factory=list)
    total_count: Optional[int] = Field(None)