################# 필요 라이브러리 loading #################

# install.packages( c("dplyr", "ggplot2", "maps", "mapdata") )
library(ggplot2)
library(maps)
library(mapdata)
library(lattice)

# Spark Setting : Standalone

if (nchar(Sys.getenv("SPARK_HOME")) < 1) {
  Sys.setenv(SPARK_HOME = "/home/niceguy1575/spark")
}
library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
sparkR.session(master = "local[*]", sparkConfig = list(spark.driver.memory = "2g"))
sparkR.session(sparkPackages = "com.databricks:spark-avro_2.11:3.0.0")

# data loading
dataPath <- "hdfs://niceguy1575:9000/input"
airData <- read.df(dataPath, "csv", header = "true", inferSchema = "true", na.strings = "NA")

################# 필요 dataSave #################

# SQL setup
createOrReplaceTempView(airData, "temp_df")

######## 총 운행횟수
total_flight = nrow(airData)

######## 항공기 운행에 이용된 공항의 총 갯수             
  originQuery <- "SELECT DISTINCT Year, Origin
                    FROM temp_df
                    GROUP BY Year, Origin"
  # get year and make data
    originDf <- as.data.frame( sql(originQuery) )
  # write table
    write.csv(originDf,"/home/niceguy1575/data/airline/results/OriginDf.csv")
    
    
######## 연간 Arrival & Departure 평균 지연시간 및 운행횟수
  delayQuery <- "SELECT Year, COUNT(*) AS CNT, SUM(ArrDelay) AS ArrDelaySum, SUM(DepDelay) AS DepDealySum
              FROM temp_df
              GROUP BY Year"
  delay_df = as.data.frame(sql(delayQuery) )
  #write table
  write.csv(delay_df,"/home/niceguy1575/data/airline/results/delay_df.csv")
  
######## 연간 Arrival & Departure 평균 지연시간 및 운행횟수
  airportsQuery <- "SELECT Year, Origin, Dest, COUNT(DepDelay) AS CNT,
                  SUM(DepDelay) AS DepDelaySum, SUM(ArrDelay) AS ArrDelaySum
                  FROM temp_df
                  GROUP BY Year, Origin, Dest"
  airports_df = as.data.frame(sql(airportsQuery) )
  #write table
  write.csv(airports_df,"/home/niceguy1575/data/airline/results/airports_df.csv")

######## ack df
  ACKQuery <- "SELECT Year, Dest, AVG(DepDelay) as DepDelayMean, AVG(ArrDelay) as ArrDelayMean
              FROM temp_df
              WHERE Origin = 'ACK'
              GROUP BY Year, Dest"
  ACKdf <- as.data.frame( sql(ACKQuery) )
  #write table
  write.csv(ACKdf,"/home/niceguy1575/data/airline/results/ACKdf.csv")

######## airportsDelay  
 # for using dplyr, detach SparkR
  detach("package:SparkR", unload=TRUE)
  library(dplyr)
  airports_df = tbl_df(airports_df)
  Origins = unique(airports_df$Origin)
  airportsDelay = data.frame()
  for (i in 1:length(Origins) ) {
    temp <- airports_df %>%
            group_by(Year) %>%
            filter(Origin==Origins[i] | Dest==Origins[i]) %>%
            summarise(CNT = sum(CNT), DepDelaySum = sum(DepDelaySum, na.rm = T), ArrDelaySum = sum(ArrDelaySum, na.rm = T),
                      DepDelayMean = sum(DepDelaySum, na.rm = T)/sum(CNT),
                      ArrDelayMean = sum(ArrDelaySum, na.rm = T)/sum(CNT) )
    temp$airports = Origins[i]
    airportsDelay = rbind(airportsDelay, temp)
    rm(temp); gc();
  }
  # write table
  write.csv(airportsDelay,"/home/niceguy1575/data/airline/results/airportsDelay.csv")

######## find outlier
  depOver = delaySummary[delaySummary$depMean >= 20,c('airports','depMean')]
  arrOver = delaySummary[delaySummary$arrMean >= 20,c('airports','arrMean')]
  over20 = unique(c( depOver$airports, arrOver$airports ))
  delaySummary = airportsDelay %>%
                   group_by(airports) %>%
                   summarise(depMean = sum(DepDelaySum, na.rm = T)/sum(CNT), 
                              arrMean = sum(ArrDelaySum, na.rm = T)/sum(CNT) )
  # write table
  write.csv(delaySummary,"/home/niceguy1575/data/airline/results/delaySummary.csv")