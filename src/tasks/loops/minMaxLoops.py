
def run(rdd):
    data = rdd.collect()

    if not data:
        return {
            "min": None,
            "max": None,
            "avg": None
        }

    min_size = float("inf")
    max_size = float("-inf")
    total = 0
    count = 0

    for row in data:
        size = row["size"]

        if size < min_size:
            min_size = size

        if size > max_size:
            max_size = size

        total += size
        count += 1

    avg_size = total / count if count != 0 else 0

    return {
        "min": min_size,
        "max": max_size,
        "avg": avg_size
    }