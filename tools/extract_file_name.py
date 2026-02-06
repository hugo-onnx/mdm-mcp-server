# extract_file_name_tool.py
from ibm_watsonx_orchestrate.agent_builder.tools import tool
import json
import re


@tool()
def extract_file_name(output: str) -> str:
    """Extracts the file_name from a watsonx orchestrate job output.

    Args:
        output (str): The string representation of a watsonx orchestrate job response.

    Returns:
        str: The extracted file_name, or an error message if not found.
    """
    try:
        # Strategy 1: Try direct JSON parsing (in case it's a clean JSON string)
        try:
            parsed = json.loads(output)
            if isinstance(parsed, dict):
                if 'file_name' in parsed:
                    return str(parsed['file_name'])
                if 'structuredContent' in parsed:
                    file_name = parsed['structuredContent'].get('result', {}).get('file_name')
                    if file_name:
                        return str(file_name)
                if 'result' in parsed:
                    file_name = parsed['result'].get('file_name')
                    if file_name:
                        return str(file_name)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract from structuredContent in repr string
        structured_match = re.search(r"structuredContent=\{[^}]*'result':\s*\{[^}]*'file_name':\s*'([^']+)'", output)
        if structured_match:
            return structured_match.group(1)
        
        # Strategy 3: Extract from TextContent text field (JSON inside the repr)
        text_match = re.search(r"text='(\{[^']*\"file_name\"[^']*\})'", output)
        if text_match:
            try:
                text_json = text_match.group(1)
                parsed = json.loads(text_json)
                if 'file_name' in parsed:
                    return str(parsed['file_name'])
            except json.JSONDecodeError:
                pass
        
        # Strategy 4: Direct regex for file_name with double quotes (JSON style)
        file_name_match = re.search(r'"file_name"\s*:\s*"([^"]+)"', output)
        if file_name_match:
            return file_name_match.group(1)
        
        # Strategy 5: Direct regex for file_name with single quotes (Python repr style)
        file_name_match = re.search(r"'file_name'\s*:\s*'([^']+)'", output)
        if file_name_match:
            return file_name_match.group(1)
        
        return "Error: file_name not found in the provided output"
    
    except Exception as e:
        return f"Error: {str(e)}"