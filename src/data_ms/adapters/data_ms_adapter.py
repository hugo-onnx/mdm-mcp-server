# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Data Microservice adapter for IBM MDM MCP server.

This module provides an adapter for communicating with the Data Microservice,
handling entities, records, search operations, and data exports.

MODIFIED: Added better error capturing for create_data_export.
"""

import logging
import json
import requests
from typing import Dict, Any, Optional, List

from common.core.base_adapter import BaseMDMAdapter

logger = logging.getLogger(__name__)


class DataMSAdapter(BaseMDMAdapter):
    """
    Adapter for Data Microservice endpoints.
    
    This adapter provides methods for interacting with the Data Microservice:
    - Entity operations (get entity by ID)
    - Record operations (get record by ID, get entities for record)
    - Search operations (search records, entities, relationships, hierarchy nodes)
    - Export operations (create export, get export, download export)
    
    All methods use the base adapter's HTTP execution methods and handle
    Data MS-specific endpoint construction and parameter formatting.
    """
    
    def get_entity(
        self,
        entity_id: str,
        crn: str
    ) -> Dict[str, Any]:
        """
        Get an entity by ID from the Data Microservice.
        
        Args:
            entity_id: The ID of the entity to retrieve
            crn: Cloud Resource Name identifying the tenant
            
        Returns:
            Entity data dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = f"entities/{entity_id}"
        params = {"crn": crn}
        
        self.logger.info(f"Fetching entity {entity_id} for CRN: {crn}")
        return self.execute_get(endpoint, params)
    
    def get_record(
        self,
        record_id: str,
        crn: str
    ) -> Dict[str, Any]:
        """
        Get a record by ID from the Data Microservice.
        
        Args:
            record_id: The ID of the record to retrieve
            crn: Cloud Resource Name identifying the tenant
            
        Returns:
            Record data dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = f"records/{record_id}"
        params = {"crn": crn}
        
        self.logger.info(f"Fetching record {record_id} for CRN: {crn}")
        return self.execute_get(endpoint, params)
    
    def get_record_entities(
        self,
        record_id: str,
        crn: str
    ) -> Dict[str, Any]:
        """
        Get all entities for a record from the Data Microservice.
        
        Args:
            record_id: The ID of the record to retrieve entities for
            crn: Cloud Resource Name identifying the tenant
            
        Returns:
            Entities data dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = f"records/{record_id}/entities"
        params = {"crn": crn}
        
        self.logger.info(f"Fetching entities for record {record_id} for CRN: {crn}")
        return self.execute_get(endpoint, params)
    
    def search_master_data(
        self,
        search_criteria: Dict[str, Any],
        crn: str,
        limit: int = 10,
        offset: int = 0,
        include_total_count: bool = True,
        include_attributes: Optional[List[str]] = None,
        exclude_attributes: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Search for master data (records, entities, relationships, hierarchy nodes) in the Data Microservice.
        
        Args:
            search_criteria: Search criteria dictionary containing query and filters
            crn: Cloud Resource Name identifying the tenant
            limit: Maximum number of results to return
            offset: Number of results to skip for pagination
            include_total_count: Whether to include total count in response
            include_attributes: Optional list of attributes to include in results
            exclude_attributes: Optional list of attributes to exclude from results
            
        Returns:
            Search results dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = "search"
        
        # Map search_type to return_type for the API
        search_type = search_criteria.get('search_type', 'record')
        return_type_map = {
            "record": "results",
            "entity": "results_as_entities",
            "hierarchy_node": "results_as_hierarchy_nodes",
            "relationship": "results"
        }
        return_type = return_type_map.get(search_type, "results")
        
        params: Dict[str, Any] = {
            "crn": crn,
            "limit": str(limit),
            "offset": str(offset),
            "include_total_count": str(include_total_count).lower(),
            "return_type": return_type
        }
        
        # Add include/exclude attributes if provided
        # Note: requests library handles lists by creating multiple params with same name
        # e.g., ?include=attr1&include=attr2
        if include_attributes:
            params["include"] = include_attributes
        
        if exclude_attributes:
            params["exclude"] = exclude_attributes
        
        self.logger.info(
            f"Searching {search_type} for CRN: {crn}, "
            f"return_type: {return_type}"
        )
        return self.execute_post(endpoint, search_criteria, params)
    
    def create_data_export(
        self,
        export_request: Dict[str, Any],
        crn: str,
        compression_type: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new data export job.
        
        Args:
            export_request: Export request body containing:
                - export_type: Type of data to export (record or entity)
                - format: Format of the export file (csv, tsv, psv, json)
                - file_name: Name for the export file
                - search_criteria: Search criteria for filtering (required)
            crn: Cloud Resource Name identifying the tenant
            compression_type: Compression type (tar, tgz, zip) - QUERY PARAMETER
            
        Returns:
            Export job creation response with job ID and initial status
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = "data_exports"
        
        # Build query params - compression_type is a QUERY param per API docs
        params: Dict[str, Any] = {"crn": crn}
        if compression_type:
            params["compression_type"] = compression_type
        
        self.logger.info(
            f"Creating data export job, CRN: {crn}, compression: {compression_type}"
        )
        self.logger.info(f"Request body: {json.dumps(export_request, indent=2)}")
        self.logger.info(f"Query params: {params}")
        
        # Use custom implementation to capture error response body
        url = self.build_url(endpoint)
        
        response = self._execute_request_with_retry(
            'POST',
            url,
            json=export_request,
            params=params
        )
        
        # Capture response body BEFORE raise_for_status
        response_text = None
        try:
            response_text = response.text
            self.logger.info(f"Response status: {response.status_code}")
            self.logger.info(f"Response body: {response_text}")
        except Exception as e:
            self.logger.warning(f"Could not read response text: {e}")
        
        # If error, raise with captured response
        if not response.ok:
            error_msg = f"{response.status_code} {response.reason} for url: {url}"
            if response_text:
                error_msg += f" | Response: {response_text}"
            
            # Create a custom exception that includes the response text
            error = requests.exceptions.HTTPError(error_msg, response=response)
            error.response_text = response_text  # Attach for later use
            raise error
        
        # Log transaction ID for tracing
        self._log_transaction_id(response, 'POST', endpoint)
        
        return response.json()
    
    def get_data_export(
        self,
        export_id: str,
        crn: str
    ) -> Dict[str, Any]:
        """
        Get information for a data export job.
        
        View detailed information about the specified export job including
        its current status, file information, and process IDs.
        
        Args:
            export_id: The unique identifier of the export job
            crn: Cloud Resource Name identifying the tenant
            
        Returns:
            Export job information including:
            - job_id: The export job ID
            - job_type: Type of job (e.g., 'export')
            - status: Current status (not_started, prep, queued, running, succeeded, failed, canceled, unknown)
            - export_type: Type of data being exported
            - file_name: Name of the export file
            - file_expired: Whether the file has expired
            - search_criteria: Search criteria used
            - start_time: When the job started
            - end_time: When the job completed (if finished)
            - process_ids: List of process IDs for tracking
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = f"data_exports/{export_id}"
        params = {"crn": crn}
        
        self.logger.info(
            f"Getting export job info for {export_id}, CRN: {crn}"
        )
        return self.execute_get(endpoint, params)
    
    def download_data_export(
        self,
        export_id: str,
        crn: str,
        save_to_path: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Download a completed data export file.
        
        This method downloads the actual export file content. The API returns
        the file directly as a binary stream with Content-Disposition header
        containing the filename.
        
        Args:
            export_id: The unique identifier of the export job
            crn: Cloud Resource Name identifying the tenant
            save_to_path: Optional path to save the file. If None, returns
                          the file content in the response.
            
        Returns:
            Dictionary containing:
            - export_id: The export job ID
            - file_name: Name of the downloaded file
            - content_type: MIME type of the file
            - file_size: Size of the file in bytes
            - file_content: Raw bytes content (if save_to_path is None)
            - file_path: Path where file was saved (if save_to_path is provided)
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        import re
        from config import Config
        from common.auth.authentication_manager import AuthenticationManager
        
        endpoint = f"data_exports/{export_id}/download"
        params = {"crn": crn}
        
        self.logger.info(
            f"Downloading export file for export {export_id}, CRN: {crn}"
        )
        
        base_url = Config.API_BASE_URL.rstrip('/')
        url = f"{base_url}/{endpoint}"
        
        auth_manager = AuthenticationManager()
        headers = auth_manager.get_auth_headers()
        headers["Accept"] = "application/octet-stream"
        
        response = requests.get(
            url,
            params=params,
            headers=headers,
            verify=False,
            timeout=300,
            stream=True
        )
        response.raise_for_status()
        
        content_disposition = response.headers.get("content-disposition", "")
        filename_match = re.search(r'filename="?([^";\s]+)"?', content_disposition)
        file_name = filename_match.group(1) if filename_match else f"{export_id}.csv"
        
        content_type = response.headers.get("content-type", "application/octet-stream")
        
        if save_to_path:
            import os
            full_path = os.path.join(save_to_path, file_name)
            file_size = 0
            with open(full_path, "wb") as f:
                for chunk in response.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
                        file_size += len(chunk)
            
            self.logger.info(f"Export file saved to {full_path} ({file_size} bytes)")
            
            return {
                "export_id": export_id,
                "file_name": file_name,
                "content_type": content_type,
                "file_size": file_size,
                "file_path": full_path,
                "status": "downloaded"
            }
        else:
            file_content = response.content
            
            return {
                "export_id": export_id,
                "file_name": file_name,
                "content_type": content_type,
                "file_size": len(file_content),
                "file_content": file_content,
                "status": "downloaded"
            }