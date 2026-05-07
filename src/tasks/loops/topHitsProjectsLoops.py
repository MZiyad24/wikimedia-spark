def run(rdd):

    data = rdd.collect()

    # dictionary 3shan n store total hits l kol project
    project_hits = {}

    for row in data:

        # extract project code and hits
        project = row["project"]
        hits = row["hits"]

        # law el project msh mawgood addo b 0
        if project not in project_hits:
            project_hits[project] = 0

        # ngeb total hits l kol project
        project_hits[project] += hits

    # sort descending by total hits
    sorted_projects = sorted(
        project_hits.items(),
        key=lambda x: x[1],
        reverse=True
    )

    # return top 5 projects
    top5 = sorted_projects[:5]

    # =========================
    # vertical formatting ONLY
    # =========================
    result = "\n".join(
        f"{project}: {hits}"
        for project, hits in top5
    )

    return result