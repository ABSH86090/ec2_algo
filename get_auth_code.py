from fyers_apiv3 import fyersModel
import os

client_id = os.getenv("FYERS_CLIENT_ID")
secret_key = os.getenv("FYERS_SECRET_KEY")
redirect_uri = ""

appSession = fyersModel.SessionModel(
    client_id=client_id,
    redirect_uri=redirect_uri,
    response_type="code",
    state="state_string",
    secret_key=secret_key,
    grant_type="authorization_code"
)

generateTokenUrl = appSession.generate_authcode()
print(generateTokenUrl)  # Visit this URL in your browser to login and generate auth_code
