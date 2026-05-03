from py_clob_client.client import ClobClient
import os

client = ClobClient(
    host="https://clob.polymarket.com",
    key=os.environ["PRIVATE_KEY"],
    chain_id=137
)

creds = client.create_api_key()
print("API_KEY:", creds.api_key)
print("API_SECRET:", creds.api_secret)
print("API_PASSPHRASE:", creds.api_passphrase)
