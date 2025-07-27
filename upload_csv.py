import pandas as pd
import boto3
from boto3.dynamodb.conditions import Key
import json
import sys
import os
from decimal import Decimal

class DynamoDBUploader:
    def __init__(self, config):
        # AWS credentials from environment variables
        self.dynamodb = boto3.resource(
            'dynamodb',
            region_name=config['aws']['region'],
            aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
            aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
        )
        self.table_name = config['dynamodb']['table_name']
        self.primary_key_column = config['dynamodb']['primary_key_column']
        self.region = config['aws']['region']
        
        # Validate AWS connection
        self.validate_aws_connection()
        
        # Ensure table exists
        self.table = self.ensure_table_exists()
    
    def validate_aws_connection(self):
        """Validate AWS credentials and connection"""
        try:
            sts = boto3.client(
                'sts',
                region_name=self.region,
                aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
                aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
            )
            identity = sts.get_caller_identity()
            print(f"✓ AWS connection validated for account: {identity['Account']}")
        except Exception as e:
            print(f"✗ AWS validation failed: {str(e)}")
            sys.exit(1)
    
    def table_exists(self):
        """Check if DynamoDB table exists"""
        try:
            self.dynamodb.Table(self.table_name).load()
            return True
        except:
            return False
    
    def create_table(self, sample_data):
        """Create DynamoDB table based on sample data"""
        print(f"Creating table '{self.table_name}'...")
        
        # Determine key type from sample data
        sample_value = sample_data[self.primary_key_column].iloc[0]
        key_type = 'N' if pd.api.types.is_numeric_dtype(type(sample_value)) else 'S'
        
        table = self.dynamodb.create_table(
            TableName=self.table_name,
            KeySchema=[
                {'AttributeName': self.primary_key_column, 'KeyType': 'HASH'}
            ],
            AttributeDefinitions=[
                {'AttributeName': self.primary_key_column, 'AttributeType': key_type}
            ],
            BillingMode='PAY_PER_REQUEST'
        )
        
        print("Waiting for table to be created...")
        table.wait_until_exists()
        print(f"✓ Table '{self.table_name}' created successfully")
        return table
    
    def ensure_table_exists(self):
        """Ensure table exists, create if it doesn't"""
        if self.table_exists():
            print(f"✓ Table '{self.table_name}' already exists")
            return self.dynamodb.Table(self.table_name)
        else:
            print(f"Table '{self.table_name}' does not exist")
            return None  # Will create after reading file data
    
    def transform_data(self, df):
        """
        Placeholder function for data transformation
        Add your transformation logic here when needed
        """
        # TODO: Add transformation logic
        return df
    
    def convert_to_dynamodb_format(self, item):
        """Convert pandas data types to DynamoDB compatible format"""
        for key, value in item.items():
            if pd.isna(value):
                item[key] = None
            elif isinstance(value, float):
                item[key] = Decimal(str(value))
        return item
    
    def upload_file(self, file_path):
        try:
            # Read file based on extension
            if file_path.endswith('.csv'):
                df = pd.read_csv(file_path)
            elif file_path.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file_path)
            else:
                raise ValueError("Unsupported file format. Use CSV or XLSX.")
            
            print(f"Loaded {len(df)} rows from {file_path}")
            
            # Validate primary key column exists in data
            if self.primary_key_column not in df.columns:
                raise ValueError(f"Primary key column '{self.primary_key_column}' not found in file")
            
            # Create table if it doesn't exist
            if self.table is None:
                self.table = self.create_table(df)
            
            # Apply transformation
            df = self.transform_data(df)
            
            # Upload to DynamoDB
            uploaded_count = 0
            with self.table.batch_writer() as batch:
                for _, row in df.iterrows():
                    item = row.to_dict()
                    item = self.convert_to_dynamodb_format(item)
                    
                    # Ensure primary key exists
                    if self.primary_key_column not in item or item[self.primary_key_column] is None:
                        print(f"Skipping row with missing primary key: {item}")
                        continue
                    
                    batch.put_item(Item=item)
                    uploaded_count += 1
            
            print(f"✓ Successfully uploaded {uploaded_count} items to DynamoDB table '{self.table_name}'")
            
        except Exception as e:
            print(f"✗ Error uploading file: {str(e)}")
            sys.exit(1)

def load_config():
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    with open(config_path, 'r') as f:
        return json.load(f)

def main():
    # Check for required environment variables
    if not os.getenv('AWS_ACCESS_KEY_ID') or not os.getenv('AWS_SECRET_ACCESS_KEY'):
        print("Error: AWS credentials not found in environment variables.")
        print("Please set AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY")
        sys.exit(1)
    
    if len(sys.argv) != 2:
        print("Usage: python upload_csv.py <file_path>")
        print("Example: python upload_csv.py data.csv")
        sys.exit(1)
    
    file_path = sys.argv[1]
    
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        sys.exit(1)
    
    config = load_config()
    uploader = DynamoDBUploader(config)
    uploader.upload_file(file_path)

if __name__ == "__main__":
    main()
    



# python upload_csv.py <file_path>
