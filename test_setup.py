"""Validate environment setup: API keys present and Labelbox connection works."""

import os
import sys
from dotenv import load_dotenv

load_dotenv()

errors = []

# Check env vars
for var in ("LABELBOX_API_KEY", "PLANTNET_API_KEY"):
    if not os.getenv(var):
        errors.append(f"Missing environment variable: {var}")

if errors:
    for e in errors:
        print(f"ERROR: {e}")
    sys.exit(1)

# Test Labelbox connection
try:
    import labelbox as lb
    client = lb.Client(api_key=os.environ["LABELBOX_API_KEY"])
    # Fetch the first page of datasets as a lightweight connectivity check
    next(iter(client.get_datasets()), None)
    print("OK: Labelbox connection successful")
except Exception as e:
    print(f"ERROR: Labelbox connection failed — {e}")
    sys.exit(1)

print("Setup looks good. Ready to start.")
