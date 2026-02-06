# extract_file_content_base64_tool.py
from ibm_watsonx_orchestrate.agent_builder.tools import tool
import json
import re


@tool()
def extract_file_content_base64(output: str) -> str:
    """Extracts the file_content_base64 from a watsonx orchestrate download export output.

    Args:
        output (str): The string representation of a watsonx orchestrate download response.

    Returns:
        str: The extracted base64-encoded file content, or an error message if not found.
    """
    try:
        # Strategy 1: Try direct JSON parsing (in case it's a clean JSON string)
        try:
            parsed = json.loads(output)
            if isinstance(parsed, dict):
                if 'file_content_base64' in parsed:
                    return str(parsed['file_content_base64'])
                if 'structuredContent' in parsed:
                    content = parsed['structuredContent'].get('result', {}).get('file_content_base64')
                    if content:
                        return str(content)
                if 'result' in parsed:
                    content = parsed['result'].get('file_content_base64')
                    if content:
                        return str(content)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract from TextContent text field (JSON inside the repr)
        # The base64 content can be very long, so we use a pattern that captures until the next key or end
        text_match = re.search(r"text='(\{.*\})'", output, re.DOTALL)
        if text_match:
            try:
                text_json = text_match.group(1)
                parsed = json.loads(text_json)
                if 'file_content_base64' in parsed:
                    return str(parsed['file_content_base64'])
            except json.JSONDecodeError:
                pass
        
        # Strategy 3: Direct regex for file_content_base64 with double quotes (JSON style)
        # Base64 contains alphanumeric, +, /, and = characters
        content_match = re.search(r'"file_content_base64"\s*:\s*"([A-Za-z0-9+/=]+)"', output)
        if content_match:
            return content_match.group(1)
        
        # Strategy 4: Direct regex for file_content_base64 with single quotes (Python repr style)
        content_match = re.search(r"'file_content_base64'\s*:\s*'([A-Za-z0-9+/=]+)'", output)
        if content_match:
            return content_match.group(1)
        
        return "Error: file_content_base64 not found in the provided output"
    
    except Exception as e:
        return f"Error: {str(e)}"