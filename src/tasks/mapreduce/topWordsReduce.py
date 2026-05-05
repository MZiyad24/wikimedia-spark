import re

def normalize(word):
    return re.sub(r'[^a-z0-9]', '', word.lower())

def run(rdd):
    result = (
        rdd
        .flatMap(lambda x: x["title"].split("_"))           # split by "_"
        .map(lambda w: normalize(w))                         # lowercase + remove non-alphanumeric
        .filter(lambda w: len(w) > 0)                        # remove empty strings
        .map(lambda w: (w, 1))                               # emit (word, 1)
        .reduceByKey(lambda a, b: a + b)                     # sum counts per word
        .sortBy(lambda x: -x[1])                             # sort descending
        .take(10)                                            # top 10
    )
    return "\n".join([f"{term}: {count}" for term, count in result])