def run(rdd):

    result = (
        rdd

        # remove invalid rows
        .filter(
            lambda x:
            x.get("project")
            and x.get("title")
            and x.get("hits")
        )

        # (project, (title, hits))
        .map(
            lambda x: (
                x["project"],
                (x["title"], int(x["hits"]))
            )
        )

        # choose page with max hits
        .reduceByKey(
            lambda a, b:
            a if a[1] > b[1] else b
        )

        .collect()
    )

    # vertical formatting
    formatted = "\n".join(
        [
            f"{project} -> {title}: {hits}"
            for project, (title, hits) in result
        ]
    )

    return formatted