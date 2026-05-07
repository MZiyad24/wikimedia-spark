def run(rdd):
    return (
        rdd
        # remove invalid rows
        .filter(lambda x: x.get("project") and x.get("hits"))
        # transform row -> (project, hits)
        .map(lambda x: (x["project"], int(x["hits"])))
        # combine hits for same project
        .reduceByKey(lambda a, b: a + b)
        #(project, total_hits)
        .map(lambda x: (x[0], x[1]))
        # get top 5 descending by hits
        .takeOrdered(5, key=lambda x: -x[1])
    )
