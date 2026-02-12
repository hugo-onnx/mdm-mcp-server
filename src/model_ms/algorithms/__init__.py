# Copyright [2026] [IBM]
# Licensed under the Apache License, Version 2.0 (http://www.apache.org/licenses/LICENSE-2.0)
# See the LICENSE file in the project root for license information.

"""
Algorithms module for IBM MDM MCP server.

This module provides tools and services for working with matching algorithms
in IBM Master Data Management.
"""

from .tools import get_matching_algorithm
from .service import AlgorithmService

__all__ = [
    'get_matching_algorithm',
    'AlgorithmService'
]