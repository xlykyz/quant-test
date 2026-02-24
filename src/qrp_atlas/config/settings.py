import os

DB_READ_ONLY = os.getenv("QRP_DB_READ_ONLY", "0") == "1"
