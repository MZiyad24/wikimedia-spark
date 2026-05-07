import os
from src.config import RESULTS_DIR

def is_image(title):
    title = title.lower()
    return title.endswith((".jpg", ".png", ".gif"))

def save_to_file(filename, content, mode="w"):

    # create results folder if not exists
    os.makedirs(RESULTS_DIR, exist_ok=True)

    # write file with utf-8 encoding (VERY IMPORTANT)
    with open(
        f"{RESULTS_DIR}/{filename}",
        mode,
        encoding="utf-8"
    ) as f:
        f.write(str(content))