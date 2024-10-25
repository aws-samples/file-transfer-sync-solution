import json, os, subprocess, sys
from aws_cdk import (
    Duration,
    Stack,
    RemovalPolicy,
    CfnTag,
    aws_s3 as s3,
    aws_sns as sns,
    aws_iam as iam,
    aws_kms as kms,
    aws_secretsmanager as secretmanager,
    aws_transfer as transfer,
    aws_scheduler as scheduler,
    aws_lambda as lambda_,
    aws_stepfunctions as sfn,
    aws_stepfunctions_tasks as sfn_tasks,
    aws_logs as logs,
)
from cdk_monitoring_constructs import (
    MonitoringFacade,
    AlarmFactoryDefaults,
    SnsAlarmActionStrategy,
    ErrorCountThreshold
)
from constructs import Construct

with open('./configuration/solution_parameters/parameters.json', encoding='utf8') as solution_parameters:
    solution_parameters = json.load(solution_parameters)

boto_version = solution_parameters['boto_version']
permission_boundary_policy_arn = solution_parameters['permission_boundary_policy_arn']

class TransferSyncServiceStack(Stack):
    def install_package(package, target):
        subprocess.check_call([
            sys.executable,
            '-m',
            'pip',
            'install',
            '-q',
            package,
            '--target',
            target,
            '--no-user'
        ])

    # Install dependencies for Lambda layers if not already present
    if not os.path.exists(f'transfer_sync_service/lambda/boto3_{boto_version}_lambda_layer'):
        print('Installing some dependencies for Lambda layers')
        install_package(
            f'boto3~={boto_version}',
            f'transfer_sync_service/lambda/boto3_{boto_version}_lambda_layer/python/lib/python3.12/site-packages/'
        )

    ###
    # Need to improve this, maybe create a folder to simplify version check and updates.
    ###

    if not os.path.exists('transfer_sync_service/lambda/sync_files/pyawscron'):
        print('Installing some dependencies for Lambda')
        install_package(
            f'pyawscron~={solution_parameters["pyawscron_version"]}',
            'transfer_sync_service/lambda/sync_files/'
        )

    def files(self, path):
        """Yield all files in the given directory."""
        for file in os.listdir(path):
            if os.path.isfile(os.path.join(path, file)):
                yield file

    def cron_helper(self, expression):
        """Convert human-readable schedule expressions to cron format."""
        helpers = {
            '@monthly': '0 0 1 * ? *',
            '@daily': '0 0 * * ? *',
            '@hourly': '0 * * * ? *',
            '@minutely': '* * * * ? *',
            '@sunday': '0 0 ? * 1 *',
            '@monday': '0 0 ? * 2 *',
            '@tuesday': '0 0 ? * 3 *',
            '@wednesday': '0 0 ? * 4 *',
            '@thursday': '0 0 ? * 5 *',
            '@friday': '0 0 ? * 6 *',
            '@saturday': '0 0 ? * 7 *',
            '@every10min': '0/10 * * * ? *'
        }
        if expression in helpers:
            return helpers[expression]
        else:
            return expression

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Add Permission Boundaries if defined
        if permission_boundary_policy_arn != '':
            boundary_policy = iam.ManagedPolicy.from_managed_policy_arn(
                self, 'BoundaryPolicy', 
                managed_policy_arn=permission_boundary_policy_arn
            )
            iam.PermissionsBoundary.of(self).apply(boundary_policy)

        # Monitoring services
        # Create SNS topic for notifications
        notification_topic = sns.Topic(self, 'NotificationTopic')

        # Grant CloudWatch permissions to publish to the SNS topic
        notification_topic.add_to_resource_policy(
            statement=iam.PolicyStatement(
                actions=['sns:Publish'],
                effect=iam.Effect.ALLOW,
                principals=[iam.ServicePrincipal('cloudwatch.amazonaws.com')],
                resources=[notification_topic.topic_arn],
            )
        )

        # Set up monitoring facade
        monitoring = MonitoringFacade(
            self, "TransferSyncSolutionMonitoring",
            alarm_factory_defaults=AlarmFactoryDefaults(
                actions_enabled=True,
                alarm_name_prefix='transfer-sync',
                action=SnsAlarmActionStrategy(on_alarm_topic=notification_topic),
            )
        )

        report_kms_key = kms.Key(
            self, 'ReportKmsKey', 
            enable_key_rotation=True
        )

        # Create an S3 bucket for the SFTP Transfer Family Connector reports
        report_bucket = s3.Bucket(
            self, 'TransferSyncReportBucket',
            removal_policy=RemovalPolicy.DESTROY,
            encryption=s3.BucketEncryption.KMS,
            encryption_key=report_kms_key,
            bucket_key_enabled=True,
            auto_delete_objects=True,
            enforce_ssl=True
        )
        # Add lifecycle rule to delete old objects after 1 month
        report_bucket.add_lifecycle_rule(
            expiration=Duration.days(30),
            id='Delete old objects',
        )

        powertools_service_name = solution_parameters['powertools_service_name']
        powertools_log_level = solution_parameters['powertools_log_level']

        # Set up Lambda layers
        powertools_layer = lambda_.LayerVersion.from_layer_version_arn(
            self, 'PowerToolsLayer',
            layer_version_arn=f'arn:aws:lambda:{self.region}:017000801446:layer:AWSLambdaPowertoolsPythonV3-python312-arm64:3'
        )
        boto3_lambda_layer = lambda_.LayerVersion(
            self, 'TransferSyncBoto3LambdaLayer',
            code=lambda_.AssetCode.from_asset(os.path.join(os.getcwd(), f'transfer_sync_service/lambda/boto3_{boto_version}_lambda_layer/')),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_12],
            description=f'Boto3 Library version {boto_version} for Python 3.12',
            layer_version_name=f'python3-12-boto3-v{boto_version.replace('.','-')}'
        )

        # Create Lambda functions
        remote_server_list_lambda = lambda_.Function(
            self, 'TransferSyncRemoteServerListLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(os.path.join(os.getcwd(), 'transfer_sync_service/lambda/remote_server_list')),
            handler='remote_server_list.lambda_handler',
            timeout=Duration.seconds(60),
            memory_size=128,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                'POWERTOOLS_SERVICE_NAME': powertools_service_name,
                'POWERTOOLS_LOG_LEVEL': powertools_log_level
            },
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer, boto3_lambda_layer]
        )

        get_list_status_lambda = lambda_.Function(
            self, 'TransferSyncGetListStatusLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(os.path.join(os.getcwd(), 'transfer_sync_service/lambda/get_list_status')),
            handler='get_list_status.lambda_handler',
            timeout=Duration.seconds(60),
            memory_size=128,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                'POWERTOOLS_SERVICE_NAME': powertools_service_name,
                'POWERTOOLS_LOG_LEVEL': powertools_log_level
            },
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer]
        )
        
        report_bucket.grant_read_write(get_list_status_lambda)

        sync_files = lambda_.Function(
            self, 'TransferSyncFilesLambda',
            runtime=lambda_.Runtime.PYTHON_3_12,
            code=lambda_.AssetCode.from_asset(os.path.join(os.getcwd(), 'transfer_sync_service/lambda/sync_files')),
            handler='sync_files.lambda_handler',
            timeout=Duration.seconds(900),
            memory_size=128,
            architecture=lambda_.Architecture.ARM_64,
            environment={
                'POWERTOOLS_SERVICE_NAME': powertools_service_name,
                'POWERTOOLS_LOG_LEVEL': powertools_log_level
            },
            log_retention=logs.RetentionDays.THREE_MONTHS,
            layers=[powertools_layer, boto3_lambda_layer]
        )

        report_bucket.grant_read_write(sync_files)

        # Create scheduler role
        scheduler_role = iam.Role(
            self, 'TransferSyncSchedulerRole',
            assumed_by=iam.ServicePrincipal('scheduler.amazonaws.com')
        )

        # Create Step Functions tasks
        task_remote_server_list_lambda = sfn_tasks.LambdaInvoke(
            self, 'TaskRemoteServerListLambda',
            lambda_function=remote_server_list_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
        )

        task_get_list_status_lambda = sfn_tasks.LambdaInvoke(
            self, 'TaskGetListStatusLambda',
            lambda_function=get_list_status_lambda,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
        )

        task_sync_files_lambda = sfn_tasks.LambdaInvoke(
            self, 'TaskSyncFilesLambda',
            lambda_function=sync_files,
            integration_pattern=sfn.IntegrationPattern.REQUEST_RESPONSE,
            output_path='$.Payload',
        )

        # Create Step Functions workflow
        wait_list = sfn.Wait(
            self, 'WaitList',
            time=sfn.WaitTime.duration(Duration.seconds(5))
        )

        choice_definition = sfn.Choice(self, 'Choice')
        condition_list_pending = sfn.Condition.and_(
            sfn.Condition.is_present('$.WaitingList'),
            sfn.Condition.boolean_equals('$.WaitingList', True)
        )
        condition_recursive_list = sfn.Condition.is_present('$.RemoteFolders[0]')

        choice_definition.when(
            condition_list_pending, wait_list
        ).when(
            condition_recursive_list, task_remote_server_list_lambda
        ).otherwise(
            task_sync_files_lambda
        )

        chain_definition = task_remote_server_list_lambda.next(wait_list).next(task_get_list_status_lambda).next(choice_definition)

        map_state = sfn.Map(
            self, 'MapPerSyncSetting',
            items_path='$.SyncSettings',
            item_selector={
                'ExecutionId.$': '$$.Execution.Id',
                'Connector.$': '$.Connector',
                'ReportBucket.$': '$.ReportBucket',
                'Name.$': '$.Name',
                'SyncSetting.$': '$$.Map.Item.Value',
                'StartTime.$': '$$.Execution.StartTime',
                'Schedule.$': '$.Schedule'
            }
        ).item_processor(
            chain_definition, 
            mode=sfn.ProcessorMode.INLINE,
            execution_type=sfn.ProcessorType.STANDARD
        )

        state_machine = sfn.StateMachine(
            self, 'TransferSyncSfn',
            definition_body=sfn.DefinitionBody.from_chainable(map_state)
        )
        state_machine.grant_start_execution(scheduler_role)

        monitoring.add_large_header('Transfer Family Connectors')

        # Process configuration files
        for file in self.files('./configuration/sftp/'):
            with open(f'./configuration/sftp/{file}', encoding='utf8') as service_config:
                config = json.load(service_config)
                print(f'Creating resources for {config["Name"]}...')

                # Create a secret to store the SFTP user credentials
                secret = secretmanager.Secret(
                    self, f'transfer-sync-secret-{config["Name"]}',
                    description=config['Description'],
                    removal_policy=RemovalPolicy.DESTROY,
                    secret_name=f'aws/transfer/sync-secret-{config["Name"]}',
                    secret_object_value={} # this needs to be handled outside of CDK for security regions and to avoid changes overwriting existing secrets.
                )

                # Create IAM roles
                transfer_access_role = iam.Role(
                    self, f'transfer-sync-access-role-{config["Name"]}',
                    description=config['Description'],
                    assumed_by=iam.ServicePrincipal('transfer.amazonaws.com'),
                    role_name=f'sync-access-role-{config["Name"]}'
                )

                transfer_logging_role = iam.Role(
                    self, f'transfer-sync-logging-role-{config["Name"]}',
                    description=config['Description'],
                    assumed_by=iam.ServicePrincipal('transfer.amazonaws.com'),
                    role_name=f'sync-logging-role-{config["Name"]}'
                )

                # Grant permissions
                secret.grant_read(transfer_access_role)
                report_bucket.grant_read_write(transfer_access_role)

                # Add policies for sync settings
                for sync_setting in config['SyncSettings']:
                    transfer_access_role.add_to_policy(
                        iam.PolicyStatement(
                            actions=[
                                's3:PutObject',
                                's3:PutObjectACL'
                            ],
                            resources=[f'arn:aws:s3:::{sync_setting["LocalRepository"]["BucketName"]}/*']
                        )
                    )
                    transfer_access_role.add_to_policy(
                        iam.PolicyStatement(
                            actions=[
                                's3:GetBucketLocation',
                                's3:ListBucket'
                            ],
                            resources=[f'arn:aws:s3:::{sync_setting["LocalRepository"]["BucketName"]}']
                        )
                    )
                    if 'KmsKeyArn' in sync_setting["LocalRepository"]:
                        transfer_access_role.add_to_policy(
                            iam.PolicyStatement(
                                actions=[
                                    'kms:Encrypt',
                                    'kms:GenerateDataKey*',
                                    'kms:Decrypt' # Decrypt is needed because of S3 multi-part uploads for big files, but the role doesn't have s3:GetObject
                                ],
                                resources=[f'{sync_setting["LocalRepository"]["KmsKeyArn"]}']
                            )
                        )
                        sync_files.role.add_to_policy(
                            iam.PolicyStatement(
                                actions=[
                                    'kms:Encrypt',
                                    'kms:GenerateDataKey*'
                                ],
                                resources=[f'{sync_setting["LocalRepository"]["KmsKeyArn"]}']
                            )
                        )
                    sync_files.role.add_to_policy(
                        iam.PolicyStatement(
                            actions=[
                                's3:GetObjectAttributes',
                                's3:GetObject',
                                's3:PutObject',
                                's3:PutObjectACL'
                            ],
                            resources=[f'arn:aws:s3:::{sync_setting["LocalRepository"]["BucketName"]}/{sync_setting["LocalRepository"]["Prefix"]}/*']
                        )
                    )

                # Add logging policy
                transfer_logging_role.add_to_policy(
                    iam.PolicyStatement(
                        actions=[
                            'logs:CreateLogStream',
                            'logs:DescribeLogStreams',
                            'logs:CreateLogGroup',
                            'logs:PutLogEvents'
                        ],
                        resources=[f'arn:aws:logs:{self.region}:{self.account}:log-group:/aws/transfer/*']
                    )
                )

                # Create Transfer Family Connector
                connector = transfer.CfnConnector(
                    self, f'transfer-sync-connector-{config["Name"]}',
                    access_role=transfer_access_role.role_arn,
                    url=f'sftp://{config["Url"]}',
                    logging_role=transfer_logging_role.role_arn,
                    security_policy_name=config['SecurityPolicyName'],
                    sftp_config=transfer.CfnConnector.SftpConfigProperty(
                        user_secret_id=secret.secret_arn,
                        trusted_host_keys=config['PublicKey']
                    ),
                    tags=[
                        CfnTag(
                            key='Name',
                            value=f'sync-connector-{config["Name"]}'
                        )
                    ]
                )

                monitoring.monitor_log(
                    log_group_name=f'/aws/transfer/{connector.attr_connector_id}',
                    human_readable_name='Error logs',
                    pattern='ERROR',
                    alarm_friendly_name='error logs',
                )

                # Grant permissions to Lambda functions
                remote_server_list_lambda.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=['transfer:StartDirectoryListing'],
                        resources=[connector.attr_arn]
                    )
                )
                sync_files.add_to_role_policy(
                    iam.PolicyStatement(
                        actions=['transfer:StartFileTransfer'],
                        resources=[connector.attr_arn]
                    )
                )

                # Create scheduler
                cron_expression = self.cron_helper(config['Schedule'])

                task_scheduler = scheduler.CfnSchedule(
                    self, f'transfer-sync-schedule-{config["Name"]}',
                    flexible_time_window= scheduler.CfnSchedule.FlexibleTimeWindowProperty(
                        mode='OFF'
                    ),
                    schedule_expression=f'cron({cron_expression})',
                    target=scheduler.CfnSchedule.TargetProperty(
                        arn=state_machine.state_machine_arn,
                        role_arn=scheduler_role.role_arn,
                        input=json.dumps({
                            'Connector': connector.attr_connector_id,
                            'ReportBucket': report_bucket.bucket_name,
                            'Name': config['Name'],
                            'SyncSettings': config['SyncSettings'],
                            'Schedule': cron_expression
                        })
                    )
                )

        monitoring.add_large_header(
            'Lambda Functions'
        ).add_small_header(
            'Remote Server List'
        ).monitor_lambda_function(
            lambda_function=remote_server_list_lambda
        ).monitor_log(
            log_group_name=f'/aws/lambda/{remote_server_list_lambda.function_name}',
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        ).add_small_header(
            'Get List Status'
        ).monitor_lambda_function(
            lambda_function=get_list_status_lambda
        ).monitor_log(
            log_group_name=f'/aws/lambda/{get_list_status_lambda.function_name}',
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        ).add_small_header(
            'Sync Remote Files'
        ).monitor_lambda_function(
            lambda_function=sync_files
        ).monitor_log(
            log_group_name=f'/aws/lambda/{sync_files.function_name}',
            human_readable_name='Error logs',
            pattern='ERROR',
            alarm_friendly_name='error logs',
        ).add_large_header(
            'Step Functions'
        ).monitor_step_function(
            state_machine=state_machine, 
            add_failed_execution_count_alarm={'Error': ErrorCountThreshold(max_error_count=1, datapoints_to_alarm=1)}
        ).add_large_header(
            'S3 Buckets'
        ).monitor_s3_bucket(
            bucket=report_bucket
        )