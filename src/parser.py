def parse_line(line):
    try:
        parts = line.strip().split()

        if len(parts) < 4:
            return None

        project = parts[0]
        hits = parts[-2]
        size = parts[-1]

        title = " ".join(parts[1:-2])

        return {
            "project": project,
            "title": title,
            "hits": int(hits),
            "size": int(size)
        }

    except:
        return None