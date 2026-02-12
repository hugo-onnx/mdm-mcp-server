# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Algorithms service for IBM MDM MCP server.

This module provides a service class that encapsulates matching algorithm-related
business logic, separating concerns from the tool interface layer and following
Hexagonal Architecture.
"""

import logging
import requests
from typing import Dict, Any, Optional

from fastmcp import Context

from common.core.base_service import BaseService
from common.domain.crn_validator import CRNValidationError
from model_ms.adapters.model_ms_adapter import ModelMSAdapter

logger = logging.getLogger(__name__)


class AlgorithmService(BaseService):
    """
    Service class for handling matching algorithm operations.
    
    This class extends BaseService and provides algorithm-specific functionality:
    - Algorithm retrieval via ModelMSAdapter
    - Algorithm-specific error handling
    
    Inherits from BaseService:
    - Session and CRN validation
    - Common error handling patterns
    
    Uses ModelMSAdapter for:
    - HTTP communication with Model Microservice
    - Algorithm endpoint operations
    
    The get_algorithm function in tools.py uses these methods to retrieve
    matching algorithms.
    """
    
    def __init__(self, adapter: Optional[ModelMSAdapter] = None):
        """
        Initialize the algorithm service with a Model MS adapter.
        
        Args:
            adapter: Optional ModelMSAdapter instance (creates default if None)
        """
        super().__init__(adapter or ModelMSAdapter())
        # Store typed adapter reference for type checking
        self.adapter: ModelMSAdapter = self.adapter  # type: ignore
    
    def fetch_algorithm_from_api(
        self,
        record_type: str,
        validated_crn: str,
        template: bool = False
    ) -> Dict[str, Any]:
        """
        Fetch matching algorithm from the IBM MDM API via adapter.
        
        Args:
            record_type: The data type identifier (e.g., 'person', 'organization')
            validated_crn: Validated Cloud Resource Name
            template: If True, returns the default template algorithm
            
        Returns:
            Algorithm data dictionary from the API
            
        Raises:
            requests.exceptions.RequestException: If API request fails
        """
        return self.adapter.get_algorithm(record_type, validated_crn, template)
    
    def get_algorithm(
        self,
        ctx: Context,
        record_type: str,
        crn: Optional[str] = None,
        template: bool = False
    ) -> Dict[str, Any]:
        """
        Get the matching algorithm for a record type with declarative validation
        and error handling.
        
        This method orchestrates the algorithm retrieval process:
        1. Validates session and CRN
        2. Fetches algorithm from API
        3. Handles errors with standardized responses
        
        Args:
            ctx: MCP Context object with session information
            record_type: The data type identifier (e.g., 'person', 'organization', 'contract')
            crn: Cloud Resource Name identifying the tenant (optional)
            template: If True, returns the default template algorithm (default: False)
            
        Returns:
            Matching algorithm data from IBM MDM or error response
        """
        try:
            # Validate session and CRN
            session_id, validated_crn, tenant_id = self.validate_session_and_crn(ctx, crn)
            
            self.logger.info(
                f"Getting matching algorithm for record_type '{record_type}', "
                f"tenant: {tenant_id} (CRN: {validated_crn}), "
                f"template: {template}, session: {session_id}"
            )
            
            # Fetch algorithm from API
            return self.fetch_algorithm_from_api(record_type, validated_crn, template)
            
        except CRNValidationError as e:
            # CRN validation errors already formatted
            return e.args[0] if e.args else {"error": str(e), "status_code": 400}
        
        except requests.exceptions.RequestException as e:
            return self.handle_api_error(
                e,
                "retrieve matching algorithm",
                {"record_type": record_type, "template": template}
            )
        
        except Exception as e:
            return self.handle_unexpected_error(e, "retrieve matching algorithm")