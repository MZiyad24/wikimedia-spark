from src.utils import is_image


def run(rdd):
    total_images = 0
    non_english_images = 0

    for record in rdd.toLocalIterator():
        if is_image(record["title"]):
            total_images += 1
            if record["project"] != "en":
                non_english_images += 1

    return (
        f"Total image page titles: {total_images}\n"
        f"Non-English image page titles: {non_english_images}"
    )
