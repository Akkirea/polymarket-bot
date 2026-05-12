"""
Derive CLOB API credentials for a Gnosis Safe wallet.

Run once, then paste the output into .env / Railway env vars.
"""
import os
from dotenv import load_dotenv
from py_clob_client.client import ClobClient

load_dotenv()

pk = os.getenv("PRIVATE_KEY") or os.getenv("PK")
if not pk:
    raise SystemExit("PK or PRIVATE_KEY not set in .env")
pk = pk if pk.startswith("0x") else "0x" + pk

funder = os.getenv("FUNDER_ADDRESS")
sig_type = int(os.getenv("SIGNATURE_TYPE", "0"))

kwargs = dict(host="https://clob.polymarket.com", key=pk, chain_id=137)
if sig_type and funder:
    kwargs["signature_type"] = sig_type
    kwargs["funder"] = funder
    print(f"Mode: Gnosis Safe  sig_type={sig_type}  funder={funder}")
else:
    print("Mode: EOA-only (no SIGNATURE_TYPE / FUNDER_ADDRESS)")

client = ClobClient(**kwargs)
creds = client.derive_api_key()
print("\nPaste these into .env and Railway environment variables:\n")
print(f"CLOB_API_KEY={creds.api_key}")
print(f"CLOB_SECRET={creds.api_secret}")
print(f"CLOB_PASS_PHRASE={creds.api_passphrase}")
