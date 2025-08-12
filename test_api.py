import requests
import json
import os
from dotenv import load_dotenv

load_dotenv()

# API configuration
BASE_URL = "http://localhost:5000/api"
AUTH_TOKEN = os.getenv('API_AUTH_TOKEN', 'fkvjabkjfbajfdvajkdfhpiuaerhgpaiubf')
HEADERS = {
    'Authorization': f'Bearer {AUTH_TOKEN}',
    'Content-Type': 'application/json'
}

def test_get_all_data():
    """Test getting all data"""
    print("Testing GET /api/crm/data...")
    response = requests.get(f"{BASE_URL}/crm/data", headers=HEADERS)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print("-" * 50)

def test_get_specific_data(email_id):
    """Test getting specific record by email"""
    print(f"Testing GET /api/crm/data/{email_id}...")
    response = requests.get(f"{BASE_URL}/crm/data/{email_id}", headers=HEADERS)
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print("-" * 50)

def test_unauthorized():
    """Test unauthorized access"""
    print("Testing unauthorized access...")
    response = requests.get(f"{BASE_URL}/data")
    print(f"Status: {response.status_code}")
    print(f"Response: {json.dumps(response.json(), indent=2)}")
    print("-" * 50)

if __name__ == "__main__":
    print("API Testing Script")
    print("=" * 50)
    
    # Test unauthorized access
    test_unauthorized()
    
    # Test authorized access
    test_get_all_data()
    
    # Test specific record (replace with actual email from your data)
    # test_get_specific_data("example@email.com")