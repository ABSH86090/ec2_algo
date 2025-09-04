import requests
import hashlib
import os
from dotenv import load_dotenv, set_key

# Load environment
env_path = ".env"
load_dotenv(dotenv_path=env_path)

# ---------------- CONFIG ----------------
client_id = os.getenv("FYERS_CLIENT_ID")
client_secret = os.getenv("FYERS_SECRET_KEY")
refresh_token = os.getenv("FYERS_REFRESH_TOKEN")
pin = os.getenv("FYERS_PIN")  # Or generate TOTP if applicable

app_id_hash = hashlib.sha256(f"{client_id}:{client_secret}".encode()).hexdigest()

url = "https://api-t1.fyers.in/api/v3/validate-refresh-token"
payload = {
    "grant_type": "refresh_token",
    "appIdHash": app_id_hash,
    "refresh_token": refresh_token,
    "pin": pin
}

response = requests.post(url, json=payload)
if response.status_code == 200:
    data = response.json()
    new_access_token = data.get('access_token')
    set_key(env_path, "FYERS_ACCESS_TOKEN", new_access_token)
else:
    print("Error:", response.text)