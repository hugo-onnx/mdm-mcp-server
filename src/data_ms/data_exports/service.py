# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Data export service for IBM MDM MCP server.

HARDCODED VERSION: Simplified for entity exports.
"""

import base64
import logging
import requests
from typing import Dict, Any, Optional

from fastmcp import Context

from common.core.base_service import BaseService
from common.domain.crn_validator import CRNValidationError
from data_ms.adapters.data_ms_adapter import DataMSAdapter

logger = logging.getLogger(__name__)


class DataExportService(BaseService):
    """
    Service class for handling data export operations.
    
    HARDCODED for entity exports with specific search_criteria structure.
    """
    
    def __init__(self, adapter: Optional[DataMSAdapter] = None):
        super().__init__(adapter or DataMSAdapter())
        self.adapter: DataMSAdapter = self.adapter
    
    def _build_hardcoded_payload(
        self,
        entity_type: str,
        file_format: str
    ) -> Dict[str, Any]:
        """
        Build the HARDCODED request body for entity export.
        
        NOTE: 
        - compression_type is a QUERY parameter, not in the body
        - file_name is optional, so we omit it (API will generate one)
        
        API expects:
        {
            "export_type": "entity",
            "format": "csv",
            "search_criteria": {
                "search_type": "entity",
                "query": {
                    "operation": "and",
                    "expressions": [{"value": "*"}]
                },
                "filters": [
                    {"type": "entity", "values": ["creditentity"]}
                ]
            }
        }
        """
        payload = {
            "export_type": "entity",
            "format": file_format,
            "search_criteria": {
                "search_type": "entity",
                "query": {
                    "operation": "and",
                    "expressions": [
                        {
                            "value": "*"
                        }
                    ]
                },
                "filters": [
                    {
                        "type": "entity",
                        "values": [
                            entity_type
                        ]
                    }
                ]
            }
        }
        
        return payload
    
    def create_export(
        self,
        ctx: Context,
        entity_type: str = "creditentity",
        file_format: str = "csv",
        compression_type: str = "zip",
        crn: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create a new data export job with HARDCODED search criteria.
        
        Args:
            ctx: MCP Context object for session tracking
            entity_type: Entity type to export (default: creditentity)
            file_format: Export file format (csv, tsv, psv, json)
            compression_type: Compression type (zip, tar, tgz) - sent as query param
            crn: Optional CRN (uses default if None)
            
        Returns:
            Export job response or formatted error response
        """
        try:
            session_id, validated_crn, tenant_id = self.validate_session_and_crn(
                ctx, crn, check_preconditions=False
            )
            
            self.logger.info(
                f"Creating export job for entity type '{entity_type}', "
                f"tenant: {tenant_id}, session: {session_id}"
            )
            
            # Build the hardcoded payload (body)
            request_body = self._build_hardcoded_payload(
                entity_type=entity_type,
                file_format=file_format
            )
            
            self.logger.info(f"Export request payload: {request_body}")
            
            # Call adapter with compression_type as separate param
            return self.adapter.create_data_export(
                export_request=request_body,
                crn=validated_crn,
                compression_type=compression_type
            )
            
        except CRNValidationError as e:
            return e.args[0] if e.args else {"error": str(e), "status_code": 400}
        
        except requests.exceptions.RequestException as e:
            # Try to extract the actual error response from MDM
            error_details = {"original_error": str(e)}
            
            # Check if we attached response_text to the exception
            if hasattr(e, 'response_text') and e.response_text:
                error_details["mdm_response"] = e.response_text
                self.logger.error(f"MDM API Error Response: {e.response_text}")
            
            # Also try to get from response object if available
            if hasattr(e, 'response') and e.response is not None:
                try:
                    error_details["status_code"] = e.response.status_code
                    error_details["response_body"] = e.response.text
                    self.logger.error(f"Response body: {e.response.text}")
                except Exception:
                    pass
            
            return {
                "error": "APIError",
                "status_code": error_details.get("status_code", 500),
                "message": f"Failed to create export job: {str(e)}",
                "details": error_details
            }
        
        except Exception as e:
            return self.handle_unexpected_error(e, "create export job")
    
    def get_export(
        self,
        ctx: Context,
        export_id: str,
        crn: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get information about an export job."""
        try:
            session_id, validated_crn, tenant_id = self.validate_session_and_crn(
                ctx, crn, check_preconditions=False
            )
            
            self.logger.info(
                f"Getting export job info for '{export_id}', "
                f"tenant: {tenant_id}, session: {session_id}"
            )
            
            return self.adapter.get_data_export(
                export_id=export_id,
                crn=validated_crn
            )
            
        except CRNValidationError as e:
            return e.args[0] if e.args else {"error": str(e), "status_code": 400}
        
        except requests.exceptions.RequestException as e:
            return self.handle_api_error(e, f"get export {export_id}")
        
        except Exception as e:
            return self.handle_unexpected_error(e, f"get export {export_id}")
    
    def download_export(
        self,
        ctx: Context,
        export_id: str,
        crn: Optional[str] = None,
        save_to_path: Optional[str] = None,
        include_base64: bool = True
    ) -> Dict[str, Any]:
        """Download an export file."""
        try:
            session_id, validated_crn, tenant_id = self.validate_session_and_crn(
                ctx, crn, check_preconditions=False
            )
            
            self.logger.info(
                f"Downloading export '{export_id}', "
                f"tenant: {tenant_id}, session: {session_id}"
            )
            
            result = self.adapter.download_data_export(
                export_id=export_id,
                crn=validated_crn,
                save_to_path=save_to_path
            )
            
            if include_base64 and "file_content" in result:
                file_content = result["file_content"]
                if isinstance(file_content, bytes):
                    result["file_content_base64"] = base64.b64encode(file_content).decode("utf-8")
                del result["file_content"]
            elif "file_content" in result:
                del result["file_content"]
            
            return result
            
        except CRNValidationError as e:
            return e.args[0] if e.args else {"error": str(e), "status_code": 400}
        
        except requests.exceptions.RequestException as e:
            return self.handle_api_error(e, f"download export {export_id}")
        
        except Exception as e:
            return self.handle_unexpected_error(e, f"download export {export_id}")