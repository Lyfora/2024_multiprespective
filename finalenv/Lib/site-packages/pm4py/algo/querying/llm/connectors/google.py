'''
    PM4Py â€“ A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschrÃ¤nkt)

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as
published by the Free Software Foundation, either version 3 of the
License, or any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Affero General Public License for more details.

You should have received a copy of the GNU Affero General Public License
along with this program.  If not, see this software project's root or
visit <https://www.gnu.org/licenses/>.

Website: https://processintelligence.solutions
Contact: info@processintelligence.solutions
'''
from enum import Enum
from pm4py.util import exec_utils
from typing import Optional, Dict, Any
import base64
import os
from pm4py.util import constants


class Parameters(Enum):
    API_KEY = "api_key"
    GOOGLE_MODEL = "google_model"
    IMAGE_PATH = "image_path"


def encode_image(image_path):
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode("utf-8")


def apply(prompt: str, parameters: Optional[Dict[Any, Any]] = None) -> str:
    import requests

    if parameters is None:
        parameters = {}

    image_path = exec_utils.get_param_value(
        Parameters.IMAGE_PATH, parameters, None
    )
    api_key = exec_utils.get_param_value(
        Parameters.API_KEY, parameters, constants.GOOGLE_API_KEY
    )
    model = exec_utils.get_param_value(
        Parameters.GOOGLE_MODEL, parameters, constants.GOOGLE_DEFAULT_MODEL
    )

    headers = {
        "Content-Type": "application/json",
    }

    payload = {"contents": [{"parts": [{"text": prompt}]}]}

    if image_path is not None:
        image_format = os.path.splitext(image_path)[1][1:].lower()
        base64_image = encode_image(image_path)
        spec = {
            "inline_data": {
                "mime_type": "image/" + image_format,
                "data": base64_image,
            }
        }
        payload["contents"][0]["parts"].append(spec)

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        + model
        + ":generateContent?key="
        + api_key
    )

    response = requests.post(url, headers=headers, json=payload).json()

    if "error" in response:
        # raise an exception when the request fails, with the provided message
        raise Exception(response["error"]["message"])

    return response["candidates"][0]["content"]["parts"][0]["text"]
