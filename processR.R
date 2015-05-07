measures <- read.csv("~/Work/Spark2/HitRate.txt", na.strings = "")

grouped <- ddply(measures, .(jobName,algType,timestamp), summarize, count=length(jobName))
startTimes <- ddply(measures, .(jobName,algType), summarize, startTime=min(timestamp))
startTimesJoined <- join(grouped, startTimes, by = c("jobName","algType"), type = "left", match = "all")
startTimesJoined$startNorm <- round((startTimesJoined$timestamp - startTimesJoined$startTime) / 20000)

measuresjoined = join(measures, startTimesJoined, by = c("jobName","algType","timestamp"), type = "left", match = "all")
visualize <- ddply(measuresjoined, .(jobName,algType,startNorm), summarize, hits=sum(hit), count=length(jobName), hitRate=hits/count)

write.csv(visualize, file = "~/Work/vizualize.csv")
