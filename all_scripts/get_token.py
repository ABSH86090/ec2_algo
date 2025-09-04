import os
import webbrowser
from http.server import BaseHTTPRequestHandler, HTTPServer
from fyers_apiv3 import fyersModel
from dotenv import load_dotenv, set_key

# Load environment
env_path = ".env"
load_dotenv(dotenv_path=env_path)

client_id = os.getenv("FYERS_CLIENT_ID")
secret_key = os.getenv("FYERS_SECRET_KEY")
redirect_uri = "http://127.0.0.1:5000"
response_type = "code"
grant_type = "authorization_code"

auth_code = None


# Local server handler to capture auth_code
class FyersAuthHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        global auth_code
        if "auth_code=" in self.path:
            auth_code = self.path.split("auth_code=")[1].split("&")[0]
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Authentication successful! You can close this window.")
        else:
            self.send_response(400)
            self.end_headers()
            self.wfile.write(b"Auth code not found!")


def start_server():
    server = HTTPServer(("127.0.0.1", 5000), FyersAuthHandler)
    server.handle_request()
    server.server_close()


def generate_new_token():
    """Runs full login flow to generate a new access token and save in .env"""
    global auth_code

    session = fyersModel.SessionModel(
        client_id=client_id,
        secret_key=secret_key,
        redirect_uri=redirect_uri,
        response_type=response_type,
        grant_type=grant_type,
    )

    # Step 1: Open login URL
    auth_url = session.generate_authcode()
    webbrowser.open(auth_url)

    # Step 2: Capture auth_code via local server
    start_server()

    # Step 3: Exchange auth_code for access_token
    session.set_token(auth_code)
    response = session.generate_token()
    access_token = response["access_token"]
    refresh_token = response["refresh_token"]

    # Step 4: Save to .env
    set_key(env_path, "FYERS_ACCESS_TOKEN", access_token)
    set_key(env_path, "FYERS_REFRESH_TOKEN", refresh_token)
    print("✅ New access token generated and saved in .env")
    return access_token,refresh_token


def ensure_token():
    """
    Ensures a valid access token is available.
    - Loads from .env if present
    - Generates new one if missing or force_refresh=True
    Returns: access_token string
    """

    token,refresh_token = generate_new_token()

    return token,refresh_token

token,refresh_token = ensure_token()
print(token)
print(refresh_token)
