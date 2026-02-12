# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Matching algorithm tools for IBM MDM MCP server.

This module provides MCP tools for retrieving and working with matching algorithms
in IBM Master Data Management.
"""

import logging
from typing import Dict, Any, Optional

from fastmcp import Context
from .service import AlgorithmService

logger = logging.getLogger(__name__)

_algorithm_service = AlgorithmService()


def get_matching_algorithm(
    ctx: Context,
    record_type: str,
    crn: Optional[str] = None,
    template: bool = False
) -> Dict[str, Any]:
    """
    Get the matching algorithm for a given record type from IBM MDM.
    
    This retrieves the matching algorithm which contains the matching metadata
    for a given record type. A matching algorithm is comprised of:
    - Standardization rules: How to normalize/clean data before matching
    - Bucket generation: How to group similar records for comparison
    - Comparison rules: How to determine if two records match
    
    Args:
        ctx: MCP Context object (automatically injected) - provides session information
        record_type: The data type identifier of source records. Common values:
                     - 'person': For individual/person records
                     - 'organization': For company/organization records
                     - 'contract': For contract records
        crn: Cloud Resource Name identifying the tenant (optional, defaults to On-Prem tenant)
        template: If True, returns the default template algorithm instead of the
                  configured algorithm (default: False). Use this to see the
                  out-of-box algorithm configuration.
        
    Returns:
        Matching algorithm data containing:
        - standardizers: Collection of standardizer definitions for data normalization
        - encryption: Asymmetric encryption configuration for sensitive data
        - entity_types: Collection of entity type definitions
        - locale: The request language and location (e.g., 'enUS')
        - bucket_group_bit_length: Bit length for bucket group (default: 4)
        
    Examples:
        # Get the configured matching algorithm for person records
        algorithm = get_matching_algorithm(record_type="person")
        
        # Get the default template algorithm for organizations
        template_algo = get_matching_algorithm(
            record_type="organization",
            template=True
        )
        
        # Get algorithm for a specific tenant using full CRN
        algorithm = get_matching_algorithm(
            record_type="person",
            crn="crn:v1:staging:public:mdm-oc:us-south:a/account123:instance456::"
        )
        
    Use Cases:
        - Understanding how records of a certain type are matched
        - Reviewing standardization rules before data import
        - Comparing default vs. customized matching logic
        - Troubleshooting matching issues by examining algorithm configuration
    """
    return _algorithm_service.get_algorithm(ctx, record_type, crn, template)