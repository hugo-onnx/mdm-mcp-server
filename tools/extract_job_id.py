#extract_job_id_tool.py
from ibm_watsonx_orchestrate.agent_builder.tools import tool
import json
import re
import ast


@tool()
def extract_job_id(output: str) -> str:
    """Extracts the job_id from a watsonx orchestrate job output.

    Args:
        output (str): The string representation of a watsonx orchestrate job response.

    Returns:
        str: The extracted job_id, or an error message if not found.
    """
    try:
        # Strategy 1: Try direct JSON parsing (in case it's a clean JSON string)
        try:
            parsed = json.loads(output)
            if isinstance(parsed, dict):
                if 'job_id' in parsed:
                    return str(parsed['job_id'])
                if 'structuredContent' in parsed:
                    job_id = parsed['structuredContent'].get('result', {}).get('job_id')
                    if job_id:
                        return str(job_id)
                if 'result' in parsed:
                    job_id = parsed['result'].get('job_id')
                    if job_id:
                        return str(job_id)
        except json.JSONDecodeError:
            pass
        
        # Strategy 2: Extract structuredContent dict from the repr string
        # Pattern matches: structuredContent={'result': {'job_id': '123', ...}}
        structured_match = re.search(r"structuredContent=(\{[^}]*'result':\s*\{[^}]*'job_id':\s*'(\d+)')", output)
        if structured_match:
            return structured_match.group(2)
        
        # Strategy 3: Extract from TextContent text field (JSON inside the repr)
        # Pattern matches: text='{"job_id":"123",...}'
        text_match = re.search(r"text='(\{[^']*\"job_id\"[^']*\})'", output)
        if text_match:
            try:
                text_json = text_match.group(1)
                parsed = json.loads(text_json)
                if 'job_id' in parsed:
                    return str(parsed['job_id'])
            except json.JSONDecodeError:
                pass
        
        # Strategy 4: Direct regex for job_id with double quotes (JSON style)
        job_id_match = re.search(r'"job_id"\s*:\s*"(\d+)"', output)
        if job_id_match:
            return job_id_match.group(1)
        
        # Strategy 5: Direct regex for job_id with single quotes (Python repr style)
        job_id_match = re.search(r"'job_id'\s*:\s*'(\d+)'", output)
        if job_id_match:
            return job_id_match.group(1)
        
        return "Error: job_id not found in the provided output"
    
    except Exception as e:
        return f"Error: {str(e)}"