import time

def measure(func, rdd):
    start = time.time()
    result = func(rdd)
    end = time.time()
    return result, end - start