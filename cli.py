import json
import os
import re
import inquirer
from inquirer import errors
import paramiko
import colorama
from colorama import Fore, Style
import shutil
from datetime import datetime
from prettytable import PrettyTable

colorama.init()

CONFIG_DIR = "./configuration/sftp/"
BACKUP_DIR = "./configuration/backup/"

def print_colored(text, color=Fore.WHITE):
    print(f"{color}{text}{Style.RESET_ALL}")

def load_config(file_name):
    file_path = os.path.join(CONFIG_DIR, file_name)
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    return {}

def save_config(config, file_name):
    os.makedirs(CONFIG_DIR, exist_ok=True)
    file_path = os.path.join(CONFIG_DIR, file_name)
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(config, f, indent=4)
    print_colored(f"Configuration saved to {file_path}", Fore.GREEN)

def get_config_files():
    if not os.path.exists(CONFIG_DIR):
        return []
    return [f for f in os.listdir(CONFIG_DIR) if f.endswith('.json')]

def validate_url(answers, current):
    if not current:
        raise errors.ValidationError('', reason='URL cannot be empty')
    if not re.match(r'^([a-zA-Z0-9]([-a-zA-Z0-9]{0,61}[a-zA-Z0-9])?\.)+[a-zA-Z]{2,}(?::\d+)?$', current):
        raise errors.ValidationError('', reason='Invalid URL format. Please enter a valid FQDN[:port] without protocol.')
    return True

def validate_name(answers, current):
    if not current or not current.strip():
        raise errors.ValidationError('', reason='Name cannot be empty')
    return True

def safe_name(name):
    return re.sub(r'[^a-z0-9-]', '', name.lower())

def safe_bucket_name(name):
    return re.sub(r'^s3://', '', name.lower())

def safe_prefix(prefix):
    return prefix.strip('/')

def safe_remote_folder(folder):
    return '/' + folder.strip('/')

def validate_schedule(answers, current):
    if not current:
        return True
    predefined_schedules = {
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
    if current in predefined_schedules:
        return True
    
    cron_regex = r'^(\S+\s){5}\S+$'
    if re.match(cron_regex, current):
        parts = current.split()
        if len(parts) == 6:
            return True
    
    raise errors.ValidationError('', reason='Invalid schedule. Use predefined tags or a valid AWS Cron expression.')

def edit_sync_settings(sync_settings):
    while True:
        choices = [f"{s['LocalRepository']['BucketName']} -> {s['RemoteFolders']['Folder']}" for s in sync_settings]
        choices.append("Add new sync setting")
        choices.append("Finish editing")

        questions = [
            inquirer.List('choice',
                          message="Select a sync setting to edit or choose an action",
                          choices=choices,
                         )
        ]

        answers = inquirer.prompt(questions)
        choice = answers['choice']

        if choice == "Finish editing":
            if not sync_settings:
                print_colored("At least one SyncSetting is required. Please add a sync setting.", Fore.YELLOW)
                continue
            break
        elif choice == "Add new sync setting":
            new_setting = prompt_sync_setting()
            sync_settings.append(new_setting)
        else:
            index = choices.index(choice)
            updated_setting = prompt_sync_setting(sync_settings[index])
            sync_settings[index] = updated_setting

def prompt_sync_setting(existing=None):
    questions = [
        inquirer.Text('bucket_name', message="Enter BucketName",
                      default=existing['LocalRepository']['BucketName'] if existing else None),
        inquirer.Text('prefix', message="Enter Prefix",
                      default=existing['LocalRepository']['Prefix'] if existing else None),
        inquirer.Text('remote_folder', message="Enter Remote Folder (%year%, %month% and %day% tags are supported)",
                      default=existing['RemoteFolders']['Folder'] if existing else None),
        inquirer.Confirm('recursive', message="Is it recursive?",
                         default=existing['RemoteFolders']['Recursive'] if existing else True),
    ]

    answers = inquirer.prompt(questions)
    return {
        "LocalRepository": {
            "BucketName": safe_bucket_name(answers['bucket_name']),
            "Prefix": safe_prefix(answers['prefix'])
        },
        "RemoteFolders": {
            "Folder": safe_remote_folder(answers['remote_folder']),
            "Recursive": answers['recursive']
        }
    }

def fetch_host_key(hostname, port=22):
    try:
        transport = paramiko.Transport((hostname, port))
        transport.start_client()
        key = transport.get_remote_server_key()
        return f"{key.get_name()} {key.get_base64()}"
    except Exception as e:
        print_colored(f"Error fetching host key: {e}", Fore.RED)
        return None
    finally:
        if transport:
            transport.close()

def confirm_config(config):
    table = PrettyTable()
    table.field_names = ["Setting", "Value"]
    table.align["Setting"] = "r"
    table.align["Value"] = "l"
    table.max_width["Value"] = 100
    
    for key, value in config.items():
        if key != 'SyncSettings':
            table.add_row([key, str(value)])
    
    print_colored("\nConfiguration Summary:", Fore.CYAN)
    print(table)
    
    print_colored("\nSyncSettings:", Fore.CYAN)
    sync_table = PrettyTable()
    sync_table.field_names = ["Local", "Remote"]
    sync_table.align = "l"
    for setting in config.get('SyncSettings', []):
        local = f"{setting['LocalRepository']['BucketName']}/{setting['LocalRepository']['Prefix']}"
        remote = setting['RemoteFolders']['Folder']
        sync_table.add_row([local, remote])
    print(sync_table)
    
    return inquirer.confirm("Do you want to save this configuration?", default=True)

def backup_config(file_name):
    source_path = os.path.join(CONFIG_DIR, file_name)
    if os.path.exists(source_path):
        os.makedirs(BACKUP_DIR, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_name = f"{os.path.splitext(file_name)[0]}_{timestamp}.json"
        backup_path = os.path.join(BACKUP_DIR, backup_name)
        shutil.copy2(source_path, backup_path)
        print_colored(f"Backup created: {backup_path}", Fore.YELLOW)

def select_config_file():
    config_files = get_config_files()
    if not config_files:
        print_colored("No existing configuration files found.", Fore.YELLOW)
        return None
    
    return inquirer.list_input(
        message="Select a configuration file",
        choices=config_files
    )

def display_config_summary():
    table = PrettyTable()
    table.field_names = ["File Name", "Name", "URL", "Schedule"]
    for file_name in get_config_files():
        config = load_config(file_name)
        table.add_row([file_name, config.get('Name', ''), config.get('Url', ''), config.get('Schedule', '')])
    print(table)

def main():
    while True:
        questions = [
            inquirer.List('action',
                          message="Choose an action",
                          choices=['Create new configuration', 'Modify existing configuration', 'View configuration', 'Delete configuration', 'Display all configurations', 'Exit'],
                         )
        ]

        answers = inquirer.prompt(questions)

        if answers['action'] == 'Exit':
            break
        elif answers['action'] == 'Create new configuration':
            config = {}
            file_name = None
        elif answers['action'] == 'View configuration':
            file_name = select_config_file()
            if file_name:
                config = load_config(file_name)
                table = PrettyTable()
                table.field_names = ["Setting", "Value"]
                table.align["Setting"] = "r"
                table.align["Value"] = "l"
                table.max_width["Value"] = 100
                for key, value in config.items():
                    if key != 'SyncSettings':
                        table.add_row([key, str(value)])
                print(table)
                print_colored("\nSyncSettings:", Fore.CYAN)
                sync_table = PrettyTable()
                sync_table.field_names = ["Local", "Remote"]
                sync_table.align = "l"
                for setting in config.get('SyncSettings', []):
                    local = f"{setting['LocalRepository']['BucketName']}/{setting['LocalRepository']['Prefix']}"
                    remote = setting['RemoteFolders']['Folder']
                    sync_table.add_row([local, remote])
                print(sync_table)
            continue
        elif answers['action'] == 'Delete configuration':
            file_name = select_config_file()
            if file_name and inquirer.confirm(f"Are you sure you want to delete {file_name}?", default=False):
                os.remove(os.path.join(CONFIG_DIR, file_name))
                print_colored(f"Deleted {file_name}", Fore.YELLOW)
            continue
        elif answers['action'] == 'Display all configurations':
            display_config_summary()
            continue
        else:
            file_name = select_config_file()
            if not file_name:
                continue
            config = load_config(file_name)

        questions = [
            inquirer.Text('Name', message="Enter Name", validate=validate_name, default=config.get('Name', '')),
            inquirer.Text('Description', message="Enter Description", default=config.get('Description', '')),
            inquirer.Text('Url', message="Enter Url and optionally port (FQDN[:port])", validate=validate_url, default=config.get('Url', '')),
            inquirer.List('SecurityPolicyName',
                        message="Select SecurityPolicyName",
                        choices=['TransferSFTPConnectorSecurityPolicy-2024-03', 'TransferSFTPConnectorSecurityPolicy-2023-07'],
                        default=config.get('SecurityPolicyName', 'TransferSFTPConnectorSecurityPolicy-2024-03')),
        ]

        answers = inquirer.prompt(questions)
        answers['Name'] = safe_name(answers['Name'])
        config.update(answers)

        schedule_choices = [
                '@monthly', '@daily', '@hourly', '@minutely', 
                '@sunday', '@monday', '@tuesday', '@wednesday', '@thursday', '@friday', '@saturday', 
                '@every10min', 'Custom AWS Cron expression'
            ]
        
        schedule = inquirer.list_input(
            message="Select Schedule",
            choices=schedule_choices,
            default=config.get('Schedule', '@daily')
        )

        if schedule == 'Custom AWS Cron expression':
            while True:
                custom_schedule = inquirer.text(
                    message="Enter custom AWS Cron expression",
                )
                try:
                    if validate_schedule(None, custom_schedule):
                        schedule = custom_schedule
                        break
                except errors.ValidationError as e:
                    print_colored(e.reason, Fore.RED)
                    if not inquirer.confirm(message="Do you want to try again?"):
                        print_colored("Using default schedule: @daily", Fore.YELLOW)
                        schedule = '@daily'
                        break
        
        config['Schedule'] = schedule

        public_keys = config.get('PublicKey', [])
        if public_keys:
            print_colored("Existing public keys:", Fore.CYAN)
            for key in public_keys:
                print(key)
            add_new_key = inquirer.confirm(message="Do you want to add a new public key?", default=False)
        else:
            add_new_key = True
        
        if add_new_key:
            if ':' in config['Url']:
                host, port = config['Url'].split(':')
                host_key = fetch_host_key(host, int(port))
            else:
                host_key = fetch_host_key(config['Url'])
            if host_key:
                if host_key in public_keys:
                    print_colored(f"The fetched host key already exists in the configuration:", Fore.YELLOW)
                    print(host_key)
                else:
                    add_fetched_key = inquirer.confirm(
                        message=f"Do you want to add the following new host key? (Y/n) \n{host_key}",
                        default=True
                    )
                    if add_fetched_key:
                        public_keys.append(host_key)
                    else:
                        manual_key = inquirer.text(message="Enter the public key manually")
                        if manual_key and manual_key not in public_keys:
                            public_keys.append(manual_key)
                        elif manual_key in public_keys:
                            print_colored("This key already exists in the configuration.", Fore.YELLOW)
            else:
                manual_key = inquirer.text(message="Enter the public key manually")
                if manual_key and manual_key not in public_keys:
                    public_keys.append(manual_key)
                elif manual_key in public_keys:
                    print_colored("This key already exists in the configuration.", Fore.YELLOW)
        
        if public_keys:
            config['PublicKey'] = public_keys
        else:
            config.pop('PublicKey', None)  # Remove PublicKey if no keys are specified

        if 'SyncSettings' not in config:
            config['SyncSettings'] = []
        edit_sync_settings(config['SyncSettings'])

        if confirm_config(config):
            if file_name:
                backup_config(file_name)
            
            while not file_name:
                file_name = inquirer.text(message="Enter file name to save (e.g., config.json)")
                if not file_name:
                    print_colored("Please enter a valid file name.", Fore.YELLOW)
                elif not file_name.endswith('.json'):
                    file_name += '.json'
            
            save_config(config, file_name)
        else:
            print_colored("Configuration not saved.", Fore.YELLOW)

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print_colored(f"An error occurred. Please check the log file for details.", Fore.RED)