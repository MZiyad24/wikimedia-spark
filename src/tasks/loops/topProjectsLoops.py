def run(rdd):

    data = rdd.collect()

    max_pages = {}

    for row in data:

        project = row["project"]
        title = row["title"]
        hits = int(row["hits"])

        # first page in project
        if project not in max_pages:
            max_pages[project] = (title, hits)

        else:

            # compare hits
            if hits > max_pages[project][1]:
                max_pages[project] = (title, hits)

    # vertical formatting
    formatted = "\n".join(
        [
            f"{project} -> {title}: {hits}"
            for project, (title, hits) in max_pages.items()
        ]
    )

    return formatted