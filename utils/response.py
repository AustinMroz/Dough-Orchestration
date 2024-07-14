from typing import Any
from aiohttp import web

def success(
    payload: Any, 
    message: str, 
    status_bool: bool, 
    http_status: int = 200
) -> web.Response:
    response_data = {
        'payload': payload,
        'message': message,
        'status': status_bool
    }

    return web.json_response(response_data, status=http_status)