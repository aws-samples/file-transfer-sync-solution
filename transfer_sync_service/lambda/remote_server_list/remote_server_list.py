import boto3
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError
from datetime import datetime, timezone

# Initialize logger for structured logging
logger = Logger(log_uncaught_exceptions=True)

# Initialize AWS Transfer client for file operations
transfer = boto3.client('transfer')

@logger.inject_lambda_context
def lambda_handler(event, context):
    """
    Main Lambda handler function.
    
    :param event: Input event containing sync configuration
    :param context: Lambda context
    :return: Updated event with output objects
    """
    try:
        # Extract workflow ID from the execution ARN and set as correlation ID for logging
        workflow_id = event['ExecutionId'].split(':')[-1]
        logger.set_correlation_id(workflow_id)
        
        # Log incoming event for debugging purposes
        logger.debug("Received event", extra={"event": event})
        
        # Prepare output directory path, replacing '/' with '-' for S3 compatibility
        safe_remote_folder = event['SyncSetting']['RemoteFolders']['Folder'][1:].replace('/', '-')
        output_directory_path = f"{event['Name']}/{safe_remote_folder}/{workflow_id}"
        
        # Initialize OutputObjects list if not present in the event
        event.setdefault('OutputObjects', [])
        
        if 'RemoteFolders' in event:
            # Handle multiple remote folders scenario
            for remote_folder in event['RemoteFolders']:
                process_directory_listing(event, remote_folder, output_directory_path)
        else:
            # Handle single remote folder scenario
            remote_folder = event['SyncSetting']['RemoteFolders']['Folder']
            process_directory_listing(event, remote_folder, output_directory_path)
        
        # Initialize loop counter for potential use in state machine
        event['LoopCounter'] = 0
        
        logger.info("Directory listing completed successfully", extra={"output_objects": event['OutputObjects']})
        return event
    
    except KeyError as e:
        # Log and re-raise if a required key is missing in the event
        logger.error(f"Missing required key in event: {str(e)}")
        raise
    except Exception as e:
        # Log and re-raise for any unexpected errors
        logger.exception(f"Unexpected error occurred: {str(e)}")
        raise

def process_directory_listing(event, remote_folder, output_directory_path):
    """
    Process directory listing for a given remote folder.
    
    :param event: Input event containing sync configuration
    :param remote_folder: Remote folder path
    :param output_directory_path: Output directory path
    """
    try:
        # Process replaceable tags in the remote folder path
        processed_remote_folder = process_replaceable_tags(remote_folder)
        
        # Initiate directory listing using AWS Transfer
        result = transfer.start_directory_listing(
            ConnectorId=event['Connector'],
            RemoteDirectoryPath=processed_remote_folder,
            OutputDirectoryPath=f"/{event['ReportBucket']}/{output_directory_path}"
        )
        
        # Construct the full output object path
        output_object = f"{output_directory_path}/{result['OutputFileName']}"
        # Add the output object to the list in the event
        event['OutputObjects'].append(output_object)
        
        logger.info(f"Directory listing started for {processed_remote_folder}", 
                    extra={"remote_folder": processed_remote_folder, "output_object": output_object})
    
    except ClientError as e:
        # Log AWS Transfer specific errors with additional context
        logger.error(f"AWS Transfer client error for {processed_remote_folder}: {str(e)}", 
                     extra={"remote_folder": processed_remote_folder, "error_code": e.response['Error']['Code']})
        raise

def process_replaceable_tags(folder_path):
    """
    Process replaceable tags in the folder path.
    
    :param folder_path: Original folder path with potential tags
    :return: Processed folder path with replaced tags
    """
    now = datetime.now(timezone.utc)
    replacements = {
        '%year%': now.strftime('%Y'),
        '%month%': now.strftime('%m'),
        '%day%': now.strftime('%d')
    }
    
    for tag, value in replacements.items():
        folder_path = folder_path.replace(tag, value)
    
    return folder_path