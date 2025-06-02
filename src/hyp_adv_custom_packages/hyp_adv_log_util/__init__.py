# ruff: noqa

"""
Logging utility for HyperAds services.

Exports:
- base_config
- initialize_stack
- response_and_log_handler (now supports optional usr_message parameter for user-facing messages)
- parse_LambdaContext
"""

from .lambda_context_parser import parse_LambdaContext as parse_LambdaContext
from .log_handler import base_config as base_config
from .log_handler import initialize_stack as initialize_stack
from .log_handler import response_and_log_handler as response_and_log_handler
