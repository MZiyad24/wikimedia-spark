def run(rdd):

    result = (
        rdd
        # remove invalid rows
        .filter(lambda x: x.get("project") and x.get("hits"))

        # transform row -> (project, hits)
        .map(lambda x: (x["project"], int(x["hits"])))

        # combine hits for same project
        .reduceByKey(lambda a, b: a + b)

        # keep (project, total_hits)
        .map(lambda x: (x[0], x[1]))

        # top 5
        .takeOrdered(5, key=lambda x: -x[1])
    )

    # =========================
    # vertical output formatting ONLY
    # =========================
    formatted = "\n".join(
        f"{project} -> {hits}"
        for project, hits in result
    )

    return formatted