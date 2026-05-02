def parse_line(line):
    try:
        parts = line.strip().split(" ")
        if len(parts) < 4:
            return None

        return {
            "project": parts[0],
            "title": parts[1],
            "hits": int(parts[2]),
            "size": int(parts[3])
        }
    except:
        return None