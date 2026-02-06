#sleep_tool.py
import time
from ibm_watsonx_orchestrate.agent_builder.tools import tool


@tool()
def sleep_one_minute(input: str) -> str:
    """Sleeps for 1 minute before returning a response.

    Args:
        input (str): A message to include in the response after sleeping.

    Returns:
        str: A confirmation message after the sleep completes.
    """

    time.sleep(60)

    return f"Slept for 1 minute. Message: {input}"