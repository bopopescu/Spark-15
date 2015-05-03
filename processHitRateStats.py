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

	# Output
	# HitRateResults.txt file format:
	# job name overallHitRate
	# block_id overallHitRate 
	# ...
	hitRateCalculations = open("HitRateResults.txt", "w")

	print "Processing HitRate.txt ..."

	jobName = blocksHitRate.readline()

	# (k,v) = (block_id, (hits, total))
	blockIdHitMissMap = {}
	blockIdHitMissMap["total"] = (0, 0)

	for line in blocksHitRate.readlines(): 
		 it = line.split(",")
		 block_id = it[0]
		 hitMiss = it[1].split("\n")[0]

		 # calculate block hit rate
		 if (block_id in blockIdHitMissMap): # existing block
		 	(hits, total) = blockIdHitMissMap[block_id]
		 	if hitMiss == "1": # a hit
		 		blockIdHitMissMap[block_id] = (hits+1, total+1)
		 	else: # a miss
		 		blockIdHitMissMap[block_id] = (hits, total+1)
		 else: # new block
		 	blockIdHitMissMap[block_id] = (int(hitMiss), 1)

		 # calculate overall hit rate
		 (hits, total) = blockIdHitMissMap["total"]
		 if hitMiss == "1": # a hit
		 	blockIdHitMissMap["total"] = (hits+1, total+1)
	 	 else: # a miss
	 		blockIdHitMissMap["total"] = (hits, total+1)

	# write job name + overall hit rate
	hitRateCalculations.write(jobName.split("\n")[0] + " {}\n\n".format(
							  1.0 * blockIdHitMissMap["total"][0]/blockIdHitMissMap["total"][1]))

	# write the blocks + calculate hit rate
	for blockId, (hits, total) in blockIdHitMissMap.iteritems():
		if blockId != "total":
			hitRateCalculations.write(blockId + " {}\n".format(1.0 * hits / total))

	# closing the files
	print "Finished processing HitRate.txt ..."
	blocksHitRate.close()
	hitRateCalculations.close()