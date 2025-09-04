'''
    PM4Py – A Process Mining Library for Python
Copyright (C) 2024 Process Intelligence Solutions UG (haftungsbeschränkt)

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
import importlib.util


def apply(input_path, validation_path, parameters=None):
    if not importlib.util.find_spec("jsonschema"):
        raise Exception(
            "please install jsonschema in order to validate a JSONOCEL file."
        )

    import json
    import jsonschema
    from jsonschema import validate

    if parameters is None:
        parameters = {}

    file_content = json.load(open(input_path, "rb"))
    schema_content = json.load(open(validation_path, "rb"))
    try:
        validate(instance=file_content, schema=schema_content)
        return True
    except jsonschema.exceptions.ValidationError as err:
        return False
