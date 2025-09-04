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
import sys
from enum import Enum
from pm4py.util import exec_utils
from typing import Optional, Dict, Any
import base64
import os
from pm4py.util import constants


class Parameters(Enum):
    API_URL = "api_url"
    API_KEY = "api_key"
    OPENAI_MODEL = "openai_model"
    IMAGE_PATH = "image_path"
    MAX_TOKENS = "max_tokens"
    USE_RESPONSES_API = "use_responses_api"


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
        Parameters.API_KEY, parameters, constants.OPENAI_API_KEY
    )
    api_url = exec_utils.get_param_value(Parameters.API_URL, parameters, None)
    simple_content_specification = image_path is None
    max_tokens = exec_utils.get_param_value(
        Parameters.MAX_TOKENS, parameters, None
    )

    if api_url is None:
        api_url = constants.OPENAI_API_URL
    else:
        if not api_url.endswith("/"):
            api_url += "/"

    use_responses_api = exec_utils.get_param_value(Parameters.USE_RESPONSES_API, parameters, "api.openai" in api_url)

    model = exec_utils.get_param_value(
        Parameters.OPENAI_MODEL,
        parameters,
        (
            constants.OPENAI_DEFAULT_MODEL
            if image_path is None
            else constants.OPENAI_DEFAULT_VISION_MODEL
        ),
    )

    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {api_key}",
    }

    messages = []
    payload = {"model": model}

    if use_responses_api:
        messages.append(
            {"role": "user", "content": [{"type": "input_text", "text": prompt}]}
        )
    else:
        if simple_content_specification:
            messages.append({"role": "user", "content": prompt})
        else:
            messages.append(
                {"role": "user", "content": [{"type": "text", "text": prompt}]}
            )

    if image_path is not None:
        max_tokens = exec_utils.get_param_value(
            Parameters.MAX_TOKENS, parameters, 16384
        )
        image_format = os.path.splitext(image_path)[1][1:].lower()
        base64_image = encode_image(image_path)

        if use_responses_api:
            messages[0]["content"].append({
                "type": "input_image",
                "image_url": f"data:image/{image_format};base64,{base64_image}"
            })
        else:
            messages[0]["content"].append(
                {
                    "type": "image_url",
                    "image_url": {
                        "url": f"data:image/{image_format};base64,{base64_image}"
                    },
                }
            )
            payload["max_tokens"] = max_tokens

    if max_tokens is not None and not use_responses_api:
        payload["max_tokens"] = max_tokens

    if use_responses_api:
        payload["input"] = messages
    else:
        payload["messages"] = messages

    if use_responses_api:
        response = requests.post(
            api_url + "responses", headers=headers, json=payload, timeout=20*60
        )

        response = response.json()

        if "error" in response and response["error"]:
            # raise an exception when the request fails, with the provided message
            raise Exception(response["error"]["message"])

        return response["output"][-1]["content"][0]["text"]
    else:
        response = requests.post(
            api_url + "chat/completions", headers=headers, json=payload, timeout=20*60
        ).json()

        if "error" in response:
            # raise an exception when the request fails, with the provided message
            raise Exception(response["error"]["message"])

        return response["choices"][0]["message"]["content"]
