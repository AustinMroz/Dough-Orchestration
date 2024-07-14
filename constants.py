import os
from enum import Enum

from dotenv import load_dotenv

load_dotenv()

SALAD_API_KEY = os.getenv("SALAD_API_KEY", "")
INF_BACKEND_URL = os.getenv("INF_BACKEND_URL", "http://127.0.0.1:8000")
STATIC_AUTH_TOKEN = os.getenv("STATIC_AUTH_TOKEN", "static_auth")


# Enums --------
class InferenceStatus(Enum):
    QUEUED = "queued"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELED = "canceled"
