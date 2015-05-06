#
# This is for parsing the HitRate output from running a Spark Job 
# 
# After running a Spark Job (or any time between if needed)
# execute with: 
#	python processHitRateStats.py
#

if __name__ == "__main__":

	# Input 
	# HitRate.txt file format:
	# job name
	# block_id,1 = hit
	# block_id,0 = miss
	# ...
	blocksHitRate = open("HitRate.txt", "r")

	print "Processing HitRate.txt ..."

	firstline = blocksHitRate.readline().split(",")
	jobName = firstline[0]
	jobType = firstline[1].split("\n")[0]

	# Output
	# HitRateResults.txt file format:
	# job name overallHitRate
	# block_id overallHitRate 
	# ...
	fileName = "HitRateResults_" + jobName + "_" + jobType + ".txt"
	hitRateCalculations = open(fileName, "w")

	# (k,v) = (block_id, (hits, total))
	blockIdHitMissMap = {}
	blockIdHitMissMap["total"] = (0, 0)

	for line in blocksHitRate.readlines(): 
		 it = line.split(",")
		 block_id = it[0]
		 hitMiss = it[1].split("\n")[0]

		 # calculate block hit rate
		 if block_id in blockIdHitMissMap: # existing block
		 	hits, total = blockIdHitMissMap[block_id]
		 	if hitMiss == "1": # a hit
		 		blockIdHitMissMap[block_id] = (hits+1, total+1)
		 	else: # a miss
		 		blockIdHitMissMap[block_id] = (hits, total+1)
		 else: # new block
		 	blockIdHitMissMap[block_id] = (int(hitMiss), 1)

		 # calculate overall hit rate
		 hits, total = blockIdHitMissMap["total"]
		 if hitMiss == "1": # a hit
		 	blockIdHitMissMap["total"] = (hits+1, total+1)
	 	 else: # a miss
	 		blockIdHitMissMap["total"] = (hits, total+1)

	print "Printing results to file: " + fileName

	# write job name + overall hit rate
	hitRateCalculations.write(jobName + " " + jobType + " {}\n\n".format(
							  1.0 * blockIdHitMissMap["total"][0]/blockIdHitMissMap["total"][1]))

	# write the blocks + calculate hit rate
	for blockId, (hits, total) in blockIdHitMissMap.iteritems():
		if blockId != "total":
			hitRateCalculations.write(blockId + " {}\n".format(1.0 * hits / total))

	# closing the files
	print "Finished processing HitRate.txt ..."
	blocksHitRate.close()
	hitRateCalculations.close()