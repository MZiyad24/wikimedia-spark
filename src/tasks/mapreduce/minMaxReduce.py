def run(rdd):
    sizes = rdd.map(lambda x: x["size"])
    
    min_size = sizes.min()
    max_size = sizes.max()
    avg_size = sizes.mean()
    
    return min_size, max_size, avg_size