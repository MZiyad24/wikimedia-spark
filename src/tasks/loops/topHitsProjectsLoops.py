def run(rdd):

    def process_partition(rows):
        local_hits = {}

        
        # n loop 3la kol row fel partition
        for row in rows:

            project = row["project"]
            hits = row["hits"]

            # aggregate hits per project inside each partition
            
            local_hits[project] = local_hits.get(project, 0) + hits

        
        # nrg3 el results bta3t kol partition as pairs (key, value)
        return [(k, v) for k, v in local_hits.items()]

    # apply function on each partition in distributed way and collect results on driver
    
    partial_results = rdd.mapPartitions(process_partition).collect()

    project_hits = {}

    # merge all partition results into a single dictionary
  
    for project, hits in partial_results:

        project_hits[project] = project_hits.get(project, 0) + hits

    # sort projects by total hits in descending order
    sorted_projects = sorted(project_hits.items(), key=lambda x: x[1], reverse=True)

    # take top 5 projects
 
    top5 = sorted_projects[:5]

    result = ""

    # format final output 
  
    for project, hits in top5:

        result += project + ": " + str(hits) + "\n"

    return result.strip()