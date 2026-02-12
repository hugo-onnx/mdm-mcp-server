# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Model Microservice adapter for IBM MDM MCP server.

This module provides an adapter for communicating with the Model Microservice,
handling data model operations and matching algorithms.
"""

import logging
from typing import Dict, Any, Optional

from common.core.base_adapter import BaseMDMAdapter

logger = logging.getLogger(__name__)


class ModelMSAdapter(BaseMDMAdapter):
    """
    Adapter for Model Microservice endpoints.
    
    This adapter provides methods for interacting with the Model Microservice:
    - Data model operations (get data model)
    - Matching algorithm operations (get algorithm, update algorithm)
    
    All methods use the base adapter's HTTP execution methods and handle
    Model MS-specific endpoint construction and parameter formatting.
    """
    
    def get_data_model(
        self,
        crn: str,
        record_types: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get the data model from the Model Microservice.
        
        Args:
            crn: Cloud Resource Name identifying the tenant
            record_types: Optional comma-separated list of record types to retrieve
            
        Returns:
            Data model dictionary
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = "model"
        params: Dict[str, Any] = {"crn": crn}
        
        if record_types:
            params["record_types"] = record_types
        
        self.logger.info(f"Fetching data model for CRN: {crn}")
        return self.execute_get(endpoint, params)
    
    def get_algorithm(
        self,
        record_type: str,
        crn: str,
        template: bool = False
    ) -> Dict[str, Any]:
        """
        Get the matching algorithm for a given record type.
        
        This retrieves the matching algorithm which contains the matching metadata
        for a given record type and is comprised of standardization, bucket generation,
        and comparison sections.
        
        Args:
            record_type: The data type identifier of source records (e.g., 'person', 'organization', 'contract')
            crn: Cloud Resource Name identifying the tenant
            template: If True, returns the default template algorithm (default: False)
            
        Returns:
            Algorithm data dictionary containing:
            - standardizers: Collection of standardizer definitions
            - encryption: Asymmetric encryption configuration
            - entity_types: Collection of entity type definitions
            - locale: The request language and location (e.g., 'enUS')
            - bucket_group_bit_length: Bit length for bucket group (default: 4)
            
        Raises:
            requests.exceptions.RequestException: If request fails
        """
        endpoint = f"algorithms/{record_type}"
        params: Dict[str, Any] = {
            "crn": crn,
            "template": str(template).lower()
        }
        
        self.logger.info(
            f"Fetching matching algorithm for record_type '{record_type}', "
            f"CRN: {crn}, template: {template}"
        )
        return self.execute_get(endpoint, params)