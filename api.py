from flask import Flask, jsonify, request
import boto3
import os
import json
from decimal import Decimal
from dotenv import load_dotenv
from functools import wraps

load_dotenv()

app = Flask(__name__)

# Load config
def load_config():
    with open('config.json', 'r') as f:
        return json.load(f)

config = load_config()

# Initialize DynamoDB
dynamodb = boto3.resource(
    'dynamodb',
    region_name=config['aws']['region'],
    aws_access_key_id=os.getenv('AWS_ACCESS_KEY_ID'),
    aws_secret_access_key=os.getenv('AWS_SECRET_ACCESS_KEY')
)
table = dynamodb.Table(config['dynamodb']['table_name'])

# Simple auth token (in production, use proper JWT or OAuth)
AUTH_TOKEN = os.getenv('API_AUTH_TOKEN', 'fkvjabkjfbajfdvajkdfhpiuaerhgpaiubf')

def decimal_default(obj):
    """JSON serializer for Decimal objects"""
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = request.headers.get('Authorization')
        if not token or token != f'Bearer {AUTH_TOKEN}':
            return jsonify({'error': 'Invalid or missing auth token'}), 401
        return f(*args, **kwargs)
    return decorated

@app.route('/debug', methods=['GET'])
def debug_auth():
    """Debug endpoint to check auth token"""
    token = request.headers.get('Authorization')
    expected = f'Bearer {AUTH_TOKEN}'
    return jsonify({
        'received_token': token,
        'expected_token': expected,
        'auth_token_env': AUTH_TOKEN,
        'match': token == expected
    })

@app.route('/api/crm/data', methods=['GET'])
@require_auth
def get_all_data():
    """Get all data from DynamoDB table"""
    try:
        response = table.scan()
        items = response['Items']
        
        # Handle pagination if needed
        while 'LastEvaluatedKey' in response:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'])
            items.extend(response['Items'])
        
        return jsonify({
            'success': True,
            'count': len(items),
            'data': items
        }), 200, {'Content-Type': 'application/json'}
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/crm/data/<email_id>', methods=['GET'])
@require_auth
def get_data_by_email(email_id):
    """Get specific record by EmailId"""
    try:
        response = table.get_item(Key={'EmailId': email_id})
        
        if 'Item' in response:
            return jsonify({
                'success': True,
                'data': response['Item']
            })
        else:
            return jsonify({'error': 'Record not found'}), 404
    
    except Exception as e:
        return jsonify({'error': str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=5000)
    


#api/crm/data