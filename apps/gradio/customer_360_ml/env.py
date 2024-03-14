# Prompt user for credentials

# This file is used to set the settings, some from the environment variables and some from the user input. Some are also editable on the settings tab in the application.

import os
import dotenv
from dotenv import load_dotenv
load_dotenv()

PROPMPT_CREDS=False

# Shows settings tab in application
SHOW_SETTINGS=True

# Web App
PORT = 7860
BIND_HOST = '0.0.0.0'
SHARE = False
DEBUG = False

# Show Sample Code in UI
SHOW_SAMPLE_CODE = False

# Galaxy Credentials
HOST=os.environ.get("HOST")
USERNAME=os.environ.get("SB_USER")

# Galaxy Source Catalog
SOURCE_CATALOG='sample'
SOURCE_SCHEMA='burstbank'

# Target Galaxy Catalog for writing
ENABLE_WRITE = True # Setting to False will disable the write functionality
TARGET_CATALOG='s3lakehouse'

# OpenAI Configs
ENABLE_OPENAI = True # Setting to False will disable the OpenAI integration
OPENAI_MODEL = "gpt-3.5-turbo-16k"
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

