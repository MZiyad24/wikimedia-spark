from src.utils import is_image

def sequence_operation(accumulator, record):
    total_image_pages, non_english_image_pages = accumulator

    if is_image(record["title"]):
        total_image_pages += 1

        if record["project"] != "en":
            non_english_image_pages += 1
            
    return (total_image_pages, non_english_image_pages)


def combine_operation(accumulator1, accumulator2):
    return(
        accumulator1[0] + accumulator2[0],
        accumulator1[1] + accumulator2[1],
    )

def run(rdd):
    result = rdd.aggregate(
        (0, 0),
        sequence_operation,
        combine_operation
    )

    return (
        f"Total image page titles: {result[0]}\n"
        f"Non-English image page titles: {result[1]}"
    )