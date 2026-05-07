def run(rdd):
    data = rdd.collect()

    project_hits = {}

    for row in data:
        project = row["project"]
        hits = row["hits"]

        if project not in project_hits:
            project_hits[project] = 0

        project_hits[project] += hits

    sorted_projects = sorted(
        project_hits.items(),
        key=lambda x: x[1],
        reverse=True
    )

    return sorted_projects[:5]