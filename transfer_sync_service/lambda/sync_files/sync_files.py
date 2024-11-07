import boto3
import botocore
import json
from pyawscron import AWSCron
from datetime import timezone, datetime
from dateutil import parser
from aws_lambda_powertools import Logger
from typing import List, Dict, Any

# Initialize logger for structured logging
logger = Logger(log_uncaught_exceptions=True)

# Initialize AWS clients for S3 and Transfer services
s3 = boto3.client('s3')
transfer = boto3.client('transfer')
paginator = s3.get_paginator('list_objects_v2')

def partition(lst: List[Any], n: int) -> List[List[Any]]:
    """
    Yield successive n-sized chunks from lst.
    
    Args:
        lst (List[Any]): The list to be partitioned.
        n (int): The size of each partition.
    
    Returns:
        List[List[Any]]: A list of partitioned lists.
    """
    return [lst[i:i + n] for i in range(0, len(lst), n)]

@logger.inject_lambda_context
def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main Lambda handler function.
    
    Args:
        event (Dict[str, Any]): The event data passed to the Lambda function.
        context (Any): The runtime information of the Lambda function.
    
    Returns:
        Dict[str, Any]: A dictionary containing the execution results.
    """
    try:
        # Extract workflow ID for correlation
        workflow_id = event['ExecutionId'].split(':')[-1]
        logger.set_correlation_id(workflow_id)
        logger.debug("Received event", extra={"event": event})

        # Prepare paths and time-related variables
        safe_remote_folder = event['SyncSetting']['RemoteFolders']['Folder'][1:].replace('/', '-')
        output_directory_path = f"{event['Name']}/{safe_remote_folder}/{workflow_id}"
        start_time = parser.isoparse(event['StartTime']).astimezone(timezone.utc)
        schedule = event['Schedule']
        # Calculate the safe time to compare file modifications against
        safe_time_compare = calculate_safe_time_compare(schedule, start_time)

        # Paginate through S3 objects
        s3_pages = paginator.paginate(
            Bucket=event['ReportBucket'],
            Prefix=output_directory_path
        )

        # Check if this is the first copy operation for this destination
        first_copy = check_first_copy(event, safe_remote_folder)

        # Process S3 pages and transfer files if needed
        process_s3_pages(s3_pages, event, first_copy, safe_time_compare)

        # If it's the first copy, create a flag object in S3
        if first_copy:
            create_flag_object(event, safe_remote_folder)

        logger.info("Lambda execution completed successfully")
        return {"status": "success"}

    except Exception as e:
        logger.exception("An error occurred during Lambda execution")
        return {"status": "error", "message": str(e)}

def check_first_copy(event: Dict[str, Any], safe_remote_folder: str) -> bool:
    """
    Check if this is the first copy for the destination.
    
    Args:
        event (Dict[str, Any]): The event data.
        safe_remote_folder (str): The sanitized remote folder name.
    
    Returns:
        bool: True if it's the first copy, False otherwise.
    """
    try:
        # Try to head the flag object. If it exists, this is not the first copy.
        s3.head_object(
            Bucket=event['SyncSetting']['LocalRepository']['BucketName'],
            Key=f"{event['SyncSetting']['LocalRepository']['Prefix']}/{safe_remote_folder}.flag"
        )
        logger.info("This is not the first copy for this destination.")
        return False
    except botocore.exceptions.ClientError:
        # If the flag object doesn't exist, this is the first copy
        logger.info("This is the first copy for this destination, everything will be copied.")
        return True

def process_s3_pages(s3_pages: Any, event: Dict[str, Any], first_copy: bool, safe_time_compare: Any) -> None:
    """
    Process S3 pages and transfer files.
    
    Args:
        s3_pages (Any): The S3 page iterator.
        event (Dict[str, Any]): The event data.
        first_copy (bool): Whether this is the first copy.
        safe_time_compare (Any): The safe time to compare against.
    """
    for s3_page in s3_pages:
        for s3_object in s3_page.get('Contents', []):
            try:
                process_s3_object(s3_object, event, first_copy, safe_time_compare)
            except Exception as e:
                logger.error(f"Error processing S3 object: {s3_object['Key']}", exc_info=True)

def process_s3_object(s3_object: Dict[str, Any], event: Dict[str, Any], first_copy: bool, safe_time_compare: Any) -> None:
    """
    Process a single S3 object and transfer files if needed.
    
    Args:
        s3_object (Dict[str, Any]): The S3 object to process.
        event (Dict[str, Any]): The event data.
        first_copy (bool): Whether this is the first copy.
        safe_time_compare (Any): The safe time to compare against.
    """
    # Retrieve and parse the S3 object content
    obj = s3.get_object(Bucket=event['ReportBucket'], Key=s3_object['Key'])
    list_result = json.loads(obj['Body'].read().decode('utf-8'))
    
    if not list_result.get('files'):
        logger.info(f"No files found in S3 object: {s3_object['Key']}")
        return

    # Determine which files need to be transferred
    file_list = []
    for file in list_result['files']:
        if should_transfer_file(file, first_copy, safe_time_compare, event):
            file_list.append(file['filePath'])

    # If there are files to transfer, initiate the transfer
    if file_list:
        transfer_files(file_list, event)

def should_transfer_file(file: Dict[str, Any], first_copy: bool, safe_time_compare: Any, event: Dict[str, Any]) -> bool:
    """
    Determine if a file should be transferred.
    
    Args:
        file (Dict[str, Any]): The file information.
        first_copy (bool): Whether this is the first copy.
        safe_time_compare (Any): The safe time to compare against.
        event (Dict[str, Any]): The event data.
    
    Returns:
        bool: True if the file should be transferred, False otherwise.
    """
    file_time = parser.isoparse(file['modifiedTimestamp']).astimezone(timezone.utc)

    # If it's the first copy, transfer all files
    if first_copy:
        return True

    # For subsequent copies, check if the file is new or modified
    if safe_time_compare < file_time:
        try:
            # Check if the file exists in the destination and compare modification times
            obj = s3.head_object(
                Bucket=event['SyncSetting']['LocalRepository']['BucketName'],
                Key=f"{event['SyncSetting']['LocalRepository']['Prefix']}{file['filePath']}"
            )
            if obj['LastModified'] < file_time:
                logger.info(f"File {file['filePath']} has been modified since last copy.")
                return True
            else:
                logger.info(f"File {file['filePath']} has not been modified since last copy.")
                return False
        except botocore.exceptions.ClientError:
            # If the file doesn't exist in the destination, it should be transferred
            logger.info(f"File {file['filePath']} has not been copied before.")
            return True
    else:
        logger.info(f"File {file['filePath']} is not new.")
        return False

def transfer_files(file_list: List[str], event: Dict[str, Any]) -> None:
    """
    Transfer files using the AWS Transfer service.
    
    Args:
        file_list (List[str]): List of file paths to transfer.
        event (Dict[str, Any]): The event data.
    """
    # Partition the file list into chunks of 10
    partitioned_list = partition(file_list, 10)
    # Get the common prefix for all files in this batch
    current_prefix = file_list[0].rsplit('/', 1)[0]

    for chunk in partitioned_list:
        try:
            # Initiate file transfer for each chunk
            transfer.start_file_transfer(
                ConnectorId=event['Connector'],
                RetrieveFilePaths=chunk,
                LocalDirectoryPath=f"/{event['SyncSetting']['LocalRepository']['BucketName']}/{event['SyncSetting']['LocalRepository']['Prefix']}{current_prefix}"
            )
            logger.info(f"Started file transfer for {len(chunk)} files")
        except botocore.exceptions.ClientError as e:
            logger.error(f"Error starting file transfer: {str(e)}", exc_info=True)

def create_flag_object(event: Dict[str, Any], safe_remote_folder: str) -> None:
    """
    Create a flag object in S3 to mark the first copy.
    
    Args:
        event (Dict[str, Any]): The event data.
        safe_remote_folder (str): The sanitized remote folder name.
    """
    try:
        s3.put_object(
            Bucket=event['SyncSetting']['LocalRepository']['BucketName'],
            Key=f"{event['SyncSetting']['LocalRepository']['Prefix']}/{safe_remote_folder}.flag"
        )
        logger.info(f"Created flag object for {safe_remote_folder}")
    except botocore.exceptions.ClientError as e:
        logger.error(f"Error creating flag object: {str(e)}", exc_info=True)


def calculate_safe_time_compare(schedule: str, start_time: datetime) -> datetime:
    """
    Calculate the safe time to compare against based on the cron schedule.
    
    Args:
        schedule (str): The cron schedule string.
        start_time (datetime): The start time of the execution.
    
    Returns:
        datetime: The safe time to compare against.
    """

    # Getting the previous 2 executions time based on the cron schedule
    prev_times = AWSCron.get_prev_n_schedule(2, start_time, schedule)
    
    # Calculating the time gap between execution to accommodate execution delays safely
    start_diff = (start_time - prev_times[0]).total_seconds()
    expected_diff = (prev_times[0] - prev_times[1]).total_seconds()

    if expected_diff > start_diff:
        return prev_times[1]
    else:
        return prev_times[0]