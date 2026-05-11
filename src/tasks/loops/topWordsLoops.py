import re
from pyspark import AccumulatorParam

def normalize(word):
    return re.sub(r'[^a-z0-9]', '', word.lower())

class DictAccumulator(AccumulatorParam):
    def zero(self, value):
        return {}
    def addInPlace(self, val1, val2):
        for word, count in val2.items():
            val1[word] = val1.get(word, 0) + count
        return val1

def run(rdd):
    sc = rdd.context
    acc = sc.accumulator({}, DictAccumulator())

    def process_partition(rows):
        local_counts = {}
        for record in rows:                          # ✅ loop inside partition
            words = record["title"].split("_")
            for word in words:
                normalized = normalize(word)
                if normalized:
                    local_counts[normalized] = local_counts.get(normalized, 0) + 1
        acc.add(local_counts)                        # ✅ one update per partition

    rdd.foreachPartition(process_partition)          # ✅ distributed loop action

    top10 = sorted(acc.value.items(), key=lambda x: -x[1])[:10]
    return "\n".join([f"{term}: {count}" for term, count in top10])