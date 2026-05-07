from src.utils import is_image


def run(rdd):
    image_rdd = rdd.filter(lambda x: is_image(x["title"]))

    total_images = image_rdd.count()

    non_english_images = image_rdd.filter(lambda x: x["project"] != "en").count()

    return (
        f"Total image page titles: {total_images}\n"
        f"Non-English image page titles: {non_english_images}"
    )
