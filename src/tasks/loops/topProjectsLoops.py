def run(rdd):
    project_hits = {}

    for record in rdd.toLocalIterator():
        project = record["project"]
        hits = record["hits"]
        if project in project_hits:
            project_hits[project] += hits
        else:
            project_hits[project] = hits

    top5 = sorted(project_hits.items(), key=lambda x: -x[1])[:5]
    return "\n".join([f"{project}: {hits}" for project, hits in top5])