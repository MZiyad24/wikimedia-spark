import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

from pyspark.sql import SparkSession
from src.parser import parse_line
from src.config import DATA_PATH
from src.utils import save_to_file
from src.benchmark import measure

# Import tasks
from src.tasks.mapreduce.minMaxReduce import run as mr_t1
from src.tasks.loops.minMaxLoops import run as loop_t1

from src.tasks.mapreduce.topWordsReduce import run as mr_t3
from src.tasks.loops.topWordsLoops import run as loop_t3

from src.tasks.mapreduce.topProjectsReduce import run as mr_t5
from src.tasks.loops.topProjectsLoops import run as loop_t5

from src.tasks.mapreduce.imagesReduce import run as mr_t2
from src.tasks.loops.imagesLoops import run as loop_t2


def get_rdd(spark):
    sc = spark.sparkContext
    rdd = sc.textFile(DATA_PATH)
    return rdd.map(parse_line).filter(lambda x: x is not None)


def run_task(task_name, mr_func, loop_func, rdd):
    # Run MapReduce
    mr_result, mr_time = measure(mr_func, rdd)

    # Run Loop
    loop_result, loop_time = measure(loop_func, rdd)

    # Save task result
    output = f"""
{task_name}

MapReduce Result:
{mr_result}
Time: {mr_time:.4f} sec

Loop Result:
{loop_result}
Time: {loop_time:.4f} sec
----------------------------------------
"""
    save_to_file(f"{task_name.lower().replace(' ', '_')}.txt", output, mode="w")

    # Append benchmark
    benchmark_output = f"""
{task_name}
MapReduce: {mr_time:.4f} sec
Loop: {loop_time:.4f} sec
"""
    save_to_file("benchmark.txt", benchmark_output, mode="a")


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("WikimediaAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    rdd = get_rdd(spark)

    # Clear old benchmark
    save_to_file("benchmark.txt", "", mode="w")

    # Run tasks
    run_task("Task 1", mr_t1, loop_t1, rdd)
    run_task("Task 2", mr_t2, loop_t2, rdd)
    run_task("Task 3", mr_t3, loop_t3, rdd)
    run_task("Task 5", mr_t5, loop_t5, rdd)

    spark.stop()