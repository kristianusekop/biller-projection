
# Description & Objective
#Data extraction script to obtain the Telco data for delta 2 (Mon-Wed) in weekly basis

#1. obtain delta data
start_time <- Sys.time()

library(dplyr)
library(dbplyr)
# Connection to Postgres
con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = Sys.getenv('projdbuser'),   
                      password = Sys.getenv('projdbpass') )

library(RPostgreSQL)
TelcoHourlyQuery<-dbSendQuery(con,"select *
                              from public.trxhourlytelco where transaction_date between 
                              date(now()+interval'7 hours')-interval'3 days' and 
                              date(now()+interval'7 hours')-interval'1 days'")

telcoHourlyDd1<-as.data.frame(dbFetch(TelcoHourlyQuery))
telcoHourlyDd1<-telcoHourlyDd1[,c(1:10)]
colnames(telcoHourlyDd1)<-c("productid","productlabel","productoperator","partnerid","trxfreq","transactiondate","transactionhour","purchaseprice","platformtype","billermessage")

end_time <- Sys.time()


procTime<-end_time-start_time
procTime

dbDisconnect(con)

#2. save the data as RDS

saveRDS(telcoHourlyDd1,file="/home/rstudio/telcoHourlyD2.Rds",compress=F)
