
#DompulPr1.2

startTime<-Sys.time()

library(dplyr)

#1.set the working directory
setwd("/home/rstudio")

#2. read transaction dataset
telcoHourlyD2<-readRDS(file="telcoHourlyD2.Rds")


#3. subset data for specific Telco

#1) Axiata

KrakenBillerProductAxiata<-filter(telcoHourlyD2,productoperator %in% c("xl","axis"))
KrakenBillerProductAxiata<-filter(KrakenBillerProductAxiata,billermessage%in%c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9776","9777","9995"))


#value
#high level & dompul level
KrakenBillerProductAxiataDompulH<-as.data.frame(KrakenBillerProductAxiata%>%group_by(transactiondate)%>%summarize(trxValue=sum(purchaseprice)))
KrakenBillerProductAxiataDompul<-as.data.frame(KrakenBillerProductAxiata%>%group_by(transactiondate,billermessage)%>%summarize(trxValue=sum(purchaseprice)))

saveRDS(KrakenBillerProductAxiataDompul,file="/home/rstudio/KrakenBillerProductAxiataDompulD2.Rds",compress=F)
saveRDS(KrakenBillerProductAxiataDompulH,file="/home/rstudio/KrakenBillerProductAxiataDompulHD2.Rds",compress=F)

endTime<-Sys.time()
procTime<-endTime-startTime
procTime
