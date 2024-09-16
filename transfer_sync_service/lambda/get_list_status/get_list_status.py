import boto3
import botocore
import json
import copy
from aws_lambda_powertools import Logger
from botocore.exceptions import ClientError

# Initialize logger for structured logging
logger = Logger(log_uncaught_exceptions=True)

# Initialize S3 client for interacting with S3 buckets
s3 = boto3.client('s3')

# Constants
MAX_LOOP_COUNT = 10  # Maximum number of retry loops

@logger.inject_lambda_context
def lambda_handler(event, context):
    """
    Main Lambda handler function.
    Checks the existence of S3 objects and processes recursive listings if needed.

    :param event: Input event containing sync configuration and output objects
    :param context: Lambda context
    :return: Updated event with processing results
    """
    # Extract workflow ID from the execution ARN and set as correlation ID for logging
    workflow_id = event['ExecutionId'].split(':')[-1]
    logger.set_correlation_id(workflow_id)
    
    # Log the incoming event for debugging purposes
    logger.debug("Received event", extra={"event": event})
    
    # Create a deep copy of output objects to avoid modifying the original event
    output_objects = copy.deepcopy(event['OutputObjects'])

    # Determine whether to process recursively or not based on the sync settings
    if event['SyncSetting']['RemoteFolders']['Recursive']:
        event = process_recursive_listing(event, output_objects)
    else:
        event = process_single_listing(event, output_objects)

    # Log the processing completion status
    logger.info("Processing completed", extra={"waiting_list": event['WaitingList'], "loop_counter": event['LoopCounter']})
    return event

def process_recursive_listing(event, output_objects):
    """
    Process recursive listing of remote folders.

    :param event: The event object
    :param output_objects: List of output objects to process
    :return: Updated event object
    """
    # Initialize empty list for remote folders and set WaitingList to False
    event['RemoteFolders'] = []
    event['WaitingList'] = False

    for output_object in output_objects:
        try:
            # Retrieve and parse the content of the S3 object
            list_result = get_s3_object_content(event['ReportBucket'], output_object)
            
            # If paths are found, add them to RemoteFolders
            if list_result['paths']:
                event['RemoteFolders'].extend([path['path'] for path in list_result['paths']])
            
            # Remove the processed object from OutputObjects
            event['OutputObjects'].remove(output_object)
            logger.info(f"Processed output object", extra={"output_object": output_object})
        except ClientError as e:
            logger.warning(f"Failed to process output object", extra={"output_object": output_object, "error": str(e)})

    # If no remote folders are found but OutputObjects still exist, update the loop counter
    if not event['RemoteFolders'] and event['OutputObjects']:
        event = update_loop_counter(event)

    return event

def process_single_listing(event, output_objects):
    """
    Process single listing of remote folder.

    :param event: The event object
    :param output_objects: List of output objects to process
    :return: Updated event object
    """
    try:
        # Check if the S3 object exists
        s3.head_object(Bucket=event['ReportBucket'], Key=output_objects[0])
        event['WaitingList'] = False
        logger.info("Object found in S3", extra={"object_key": output_objects[0]})
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == "404":
            # Object not found, update loop counter and set WaitingList to True
            logger.warning("Object not available yet", extra={"object_key": output_objects[0]})
            event = update_loop_counter(event)
        elif error_code == "403":
            # Access denied error
            logger.error("Access denied", extra={"object_key": output_objects[0], "error": str(e)})
        else:
            # Any other unexpected error
            logger.error("Unexpected error", extra={"object_key": output_objects[0], "error": str(e)})

    return event

def get_s3_object_content(bucket, key):
    """
    Retrieve and parse the content of an S3 object.

    :param bucket: S3 bucket name
    :param key: S3 object key
    :return: Parsed JSON content of the S3 object
    """
    try:
        # Retrieve the S3 object
        obj = s3.get_object(Bucket=bucket, Key=key)
        # Parse and return the JSON content
        return json.loads(obj['Body'].read().decode('utf-8'))
    except ClientError as e:
        logger.error(f"Failed to get S3 object", extra={"bucket": bucket, "key": key, "error": str(e)})
        raise

def update_loop_counter(event):
    """
    Update the loop counter and set WaitingList flag.

    :param event: The event object
    :return: Updated event object
    """
    # Increment the loop counter
    event['LoopCounter'] += 1
    if event['LoopCounter'] > MAX_LOOP_COUNT:
        # If loop counter exceeds the maximum, set WaitingList to False
        event['WaitingList'] = False
        logger.error(f'Loop counter exceeded maximum allowed attempts, please check the Connector logs, connector-id: {event['Connector']}')

        # Raise an exception to fail the execution
        raise Exception(f'Loop counter exceeded maximum allowed attempts, please check the Connector logs, connector-id: {event['Connector']}')
    else:
        # Otherwise, set WaitingList to True to continue processing
        event['WaitingList'] = True
    return event