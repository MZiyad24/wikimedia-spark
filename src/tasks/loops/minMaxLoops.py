from pyspark import AccumulatorParam


class StatsAccumulator(AccumulatorParam):

    def zero(self, value):
        return {
            "min": float("inf"),
            "max": float("-inf"),
            "total": 0,
            "count": 0
        }

    def addInPlace(self, acc1, acc2):

        acc1["min"] = min(acc1["min"], acc2["min"])
        acc1["max"] = max(acc1["max"], acc2["max"])
        acc1["total"] += acc2["total"]
        acc1["count"] += acc2["count"]

        return acc1


def run(rdd):

    sc = rdd.context

    acc = sc.accumulator(
        {
            "min": float("inf"),
            "max": float("-inf"),
            "total": 0,
            "count": 0
        },
        StatsAccumulator()
    )

    def process_partition(rows):

        local_stats = {
            "min": float("inf"),
            "max": float("-inf"),
            "total": 0,
            "count": 0
        }

        for row in rows:

            size = row["size"]

            if size < local_stats["min"]:
                local_stats["min"] = size

            if size > local_stats["max"]:
                local_stats["max"] = size

            local_stats["total"] += size
            local_stats["count"] += 1

        acc.add(local_stats)

    rdd.foreachPartition(process_partition)

    result = acc.value

    if result["count"] == 0:
        return {
            "min": None,
            "max": None,
            "avg": None
        }

    return {
        "min": result["min"],
        "max": result["max"],
        "avg": result["total"] / result["count"]
    }