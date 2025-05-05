import os
from io import BytesIO
import json
from pathlib import Path
from datetime import datetime
import json
import pandas as pd
import gc


def generate_file_path(file_path: Path, file_name: str, date: datetime) -> Path:
    file_path.mkdir(parents=True, exist_ok=True)
    file_name = file_name.format(date.strftime("%Y%m%dT%H%M%S"))
    base, ext = os.path.splitext(file_name)
    final_file_path = Path.joinpath(file_path, file_name)
    idx = 1
    while final_file_path.exists():
        new_file_name = f"{base}_{idx}{ext}"
        final_file_path = Path.joinpath(file_path, new_file_name)
        idx += 1

    return final_file_path

def cleanup(obj):
    """
    Helper to delete variables that are not in used
    free up RAM for other processes

    Args :
        obj : (Any)
    """
    del obj
    gc.collect()
