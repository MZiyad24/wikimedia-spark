def run(rdd):
    result = (
        rdd
        .map(lambda x: (x["project"], x["hits"]))      # emit (project, hits)
        .reduceByKey(lambda a, b: a + b)                # sum hits per project
        .sortBy(lambda x: -x[1])                        # sort descending
        .take(5)                                         # top 5
    )
    return "\n".join([f"{project}: {hits}" for project, hits in result])