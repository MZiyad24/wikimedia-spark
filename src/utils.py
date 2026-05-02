import os
from config import RESULTS_DIR

def is_image(title):
    title = title.lower()
    return title.endswith((".jpg", ".png", ".gif"))

def save_to_file(filename, content, mode="w"):
    os.makedirs(RESULTS_DIR, exist_ok=True)
    with open(f"{RESULTS_DIR}/{filename}", mode) as f:
        f.write(content)