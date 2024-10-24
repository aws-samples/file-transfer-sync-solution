# Changelog

All notable changes to this project will be documented in this file.

## 1.1.0 - 2024-10-25
### Added features
- Added the ability to define an optional KMS Key for the target buckets in the configuration files. This allows you to set up a default encryption with KMS on the target buckets.
- Added KMS Encryption for the reporting bucket with a new dedicated key.
- Added the ability to define Permission Boundaries for all the created roles.

### Changed
- Modified `transfer_sync_service_stack` and how the lambda layer is created for boto3.
- Centralized all the solution parameters in `configuration/solution_parameters/parameters.json`.
- Updated CLI to support the new KMS Configuration.

### Dependencies
- Bump lambda-powertools from 2.40.1 to 3.2.0.
- Bump boto3 from 1.34.134 to 1.35.47 for lambda layers and environment.
- Bump cdk-monitoring-constructs from 8.1.0 to 8.3.2.

## 1.0.2 - 2024-09-25

### Added features
- Small changes to the readme file for completion on the installation process.
- Added support for ports other than 22 on the CLI and text validations.

## 1.0.1 - 2024-09-20

### Added features
- Implemented support for replaceable date tags in remote folder paths.
- New tags include %year%, %month%, and %day%, which are replaced with current UTC date values.
- This feature allows for dynamic, date-based folder selection in SFTP synchronization configurations.
- Updated README with documentation on how to use the new replaceable tags feature.

### Changed
- Modified the `process_directory_listing` function in Lambda to handle replaceable tags.
- Added a new `process_replaceable_tags` function to parse and replace date tags in folder paths.

## 1.0.0 - 2024-09-16

:seedling: Initial release.