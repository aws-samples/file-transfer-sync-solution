# Changelog

All notable changes to this project will be documented in this file.

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