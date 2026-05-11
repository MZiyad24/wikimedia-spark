from pyspark import AccumulatorParam

class MaxHitsAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}
    def addInPlace(self, val1, val2):
        for project, (title, hits) in val2.items():
            if project not in val1 \
            or hits > val1[project][1] \
            or (hits == val1[project][1] and title <= val1[project][0]):
                val1[project] = (title, hits)
        return val1


def run(rdd):

    sc = rdd.context
    acc = sc.accumulator({}, MaxHitsAccumulator())

    def process_partition(rows):
        local_max = {}
        for row in rows:
            project = row["project"]
            title = row["title"]
            hits = int(row["hits"])
            if project not in local_max \
            or hits > local_max[project][1] \
            or (hits == local_max[project][1] and title <= local_max[project][0]):
                local_max[project] = (title, hits)
        acc.add(local_max)

    rdd.foreachPartition(process_partition)

    formatted = "\n".join(
        [f"{project} -> {title}: {hits}"
         for project, (title, hits) in sorted(acc.value.items())]
    )

    return formatted