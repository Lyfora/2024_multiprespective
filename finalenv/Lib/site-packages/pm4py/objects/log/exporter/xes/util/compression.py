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
import gzip
import os
import shutil
import tempfile


# this is ugly, should be done internally in the exporter...
def compress(file):
    """
    Compress a file in-place adding .gz suffix

    Parameters
    -----------
    file
        Uncompressed file

    Returns
    -----------
    compressed_file
        Compressed file path
    """
    extension = file.split(".")[-1] + ".gz"
    fp = tempfile.NamedTemporaryFile(suffix=extension)
    fp.close()
    with open(file, "rb") as f_in:
        with gzip.open(fp.name, "wb") as f_out:
            shutil.copyfileobj(f_in, f_out)
    shutil.move(fp.name, file + ".gz")
    os.remove(file)
    return file
