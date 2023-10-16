# Prompt user for credentials
import os

PROPMPT_CREDS=False
SHOW_SETTINGS=False

# Web App
PORT = 7860
BIND_HOST = '0.0.0.0'
SHARE = False
DEBUG = False

# Credentials
HOST=os.environ.get("HOST")
USERNAME=os.environ.get("SB_USER")
PASSWORD=os.environ.get("SB_PASS")

# Target Catalog for writing
TARGET_CATALOG='s3lakehouse'

# OpenAI Configs
OPENAI_MODEL = "gpt-3.5-turbo-16k"
OPENAI_API_KEY = os.environ.get("OPEN_API_KEY")

