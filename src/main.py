from pyspark.sql import SparkSession
from parser import parse_line
from config import DATA_PATH
from utils import save_to_file
from benchmark import measure

# Import tasks
from tasks.mapreduce import task1_stats as mr_t1
from tasks.loops import task1_stats as loop_t1


def get_rdd(spark):
    sc = spark.sparkContext
    rdd = sc.textFile(DATA_PATH)
    return rdd.map(parse_line).filter(lambda x: x is not None)


def run_task(task_name, mr_func, loop_func, rdd):
    # Run MapReduce
    mr_result, mr_time = measure(mr_func.run, rdd)

    # Run Loop
    loop_result, loop_time = measure(loop_func.run, rdd)

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
    save_to_file(f"{task_name.lower().replace(' ', '_')}.txt", output)

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
    run_task("Task 1: Stats", mr_t1, loop_t1, rdd)

    spark.stop()