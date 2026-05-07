def run(rdd):
    return (
        rdd
        .filter(lambda x: x.get("project") and x.get("hits"))
        .map(lambda x: (x["project"], int(x["hits"])))
        .reduceByKey(lambda a, b: a + b)
        .map(lambda x: (x[1], x[0]))
        .takeOrdered(5, key=lambda x: -x[0])
    )