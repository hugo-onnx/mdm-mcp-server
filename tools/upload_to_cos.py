# upload_to_cos.py
import base64
import ibm_boto3
from ibm_botocore.client import Config
from ibm_watsonx_orchestrate.agent_builder.tools import tool


COS_ENDPOINT = "https://s3.eu-de.cloud-object-storage.appdomain.cloud"
COS_API_KEY = "MIPjdIYEveSdCHOd3F9UAf7-tHMzDVG8iCxBkDOrfglW"
COS_INSTANCE_CRN = "crn:v1:bluemix:public:cloud-object-storage:global:a/e759c65d85494be0be5d4aa718e7c740:a019d91b-04d2-4def-869a-23e06b68bf09:bucket:bucket-test-demo"
COS_BUCKET_NAME = "bucket-test-demo"


def get_cos_client():
    """Create and return an IBM COS client."""
    return ibm_boto3.client(
        "s3",
        ibm_api_key_id=COS_API_KEY,
        ibm_service_instance_id=COS_INSTANCE_CRN,
        config=Config(signature_version="oauth"),
        endpoint_url=COS_ENDPOINT
    )


@tool()
def upload_to_cos(file_content_base64: str, file_name: str) -> str:
    """Uploads a base64-encoded file to IBM Cloud Object Storage.

    Args:
        file_content_base64 (str): The base64-encoded file content from download_data_export.
        file_name (str): The name to give the file in COS (e.g., '6521963343265858').
                         Will automatically add .csv.zip extension if not present.

    Returns:
        str: Success message with the COS object URL, or error message.
    """
    try:
        file_bytes = base64.b64decode(file_content_base64)
        
        if not file_name.endswith('.csv.zip'):
            if file_name.endswith('.zip'):
                file_name = file_name[:-4] + '.csv.zip'
            elif '.' in file_name:
                file_name = file_name.rsplit('.', 1)[0] + '.csv.zip'
            else:
                file_name = file_name + '.csv.zip'
        
        cos_client = get_cos_client()
        
        cos_client.put_object(
            Bucket=COS_BUCKET_NAME,
            Key=file_name,
            Body=file_bytes
        )
        
        object_url = f"{COS_ENDPOINT}/{COS_BUCKET_NAME}/{file_name}"
        return f"Successfully uploaded '{file_name}' to COS. URL: {object_url}"
    
    except Exception as e:
        return f"Error uploading to COS: {str(e)}"