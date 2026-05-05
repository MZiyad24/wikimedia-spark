import re

def normalize(word):
    return re.sub(r'[^a-z0-9]', '', word.lower())

def run(rdd):
    term_counts = {}

    # Get all partitions as an iterator using toLocalIterator (no full collect)
    for record in rdd.toLocalIterator():
        words = record["title"].split("_")
        for word in words:
            normalized = normalize(word)
            if normalized:
                if normalized in term_counts:
                    term_counts[normalized] += 1
                else:
                    term_counts[normalized] = 1

    top10 = sorted(term_counts.items(), key=lambda x: -x[1])[:10]
    return "\n".join([f"{term}: {count}" for term, count in top10])