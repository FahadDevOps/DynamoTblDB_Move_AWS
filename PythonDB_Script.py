import boto3

# Source AWS account credentials
source_access_key = ''
source_secret_key = ''
source_region = 'Source_regionName' # Like eu-west-2

# Target AWS account credentials
target_access_key = ''
target_secret_key = ''
target_region = 'Target_regionName' # us-east-1

# Mapping of source table names to target table names
table_mapping = {
    'TableName1','TableName2','TableName3'
}


def export_table_schema(table_name):
    dynamodb_client = boto3.client('dynamodb', region_name=source_region, aws_access_key_id=source_access_key,
                                   aws_secret_access_key=source_secret_key)
    try:
        response = dynamodb_client.describe_table(TableName=table_name)
        return response['Table']
    except dynamodb_client.exceptions.ResourceNotFoundException:
        print(f"Table '{table_name}' not found in the source account. Skipping migration.")
        return None

def export_table_data(table_name):
    dynamodb_resource = boto3.resource('dynamodb', region_name=source_region, aws_access_key_id=source_access_key,
                                       aws_secret_access_key=source_secret_key)
    table = dynamodb_resource.Table(table_name)
    response = table.scan()
    return response['Items']

def create_target_table(table_schema, target_table_name):
    if table_schema is None:
        return

    dynamodb_client = boto3.client('dynamodb', region_name=target_region, aws_access_key_id=target_access_key,
                                   aws_secret_access_key=target_secret_key)
    try:
        dynamodb_client.describe_table(TableName=target_table_name)
        print(f"Table '{target_table_name}' already exists. Skipping table creation.")

        # Create the target table
        table_schema['TableName'] = target_table_name
        dynamodb_client.create_table(**table_schema)
        # Wait for the table to be created
        dynamodb_client.get_waiter('table_exists').wait(TableName=target_table_name)
        print(f"Table '{target_table_name}' created successfully.")

def import_table_data(target_table_name, table_data):
    if table_data is None:
        return

    dynamodb_resource = boto3.resource('dynamodb', region_name=target_region, aws_access_key_id=target_access_key,
                                       aws_secret_access_key=target_secret_key)
    table = dynamodb_resource.Table(target_table_name)
    with table.batch_writer() as batch:
        for item in table_data:
            try:
                batch.put_item(Item=item)
            except boto3.exceptions.botocore.exceptions.ClientError as e:
                error_code = e.response['Error']['Code']
                if error_code == 'ValidationException' and 'Type mismatch' in e.response['Error']['Message']:
                    # Handle type mismatch error
                    # Convert the attribute values to the correct type and retry the operation
                    converted_item = convert_item_attribute_type(item)
                    batch.put_item(Item=converted_item)
                else:
                    # Handle other errors
                    print(f"Error: {e}")
                    # You may choose to skip the item or terminate the migration process
                    # based on your specific requirements

def convert_item_attribute_type(item):
    # Convert the attribute values to the correct type based on your requirements
    # For example, if 'event_id' should be a string:
    item['event_id'] = str(item['event_id'])
    return item


# Migration process
for source_table_name, target_table_name in table_mapping.items():
    print(f"Migrating table: {source_table_name} -> {target_table_name}")
    table_schema = export_table_schema(source_table_name)
    table_data = export_table_data(source_table_name)
    create_target_table(table_schema, target_table_name)
    import_table_data(target_table_name, table_data)
    print(f"Migration complete for table: {source_table_name} -> {target_table_name}\n")
