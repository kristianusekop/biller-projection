
#Dompul_V1.1

startTime<-Sys.time()
setwd("/home/rstudio")

library(dplyr)
library(data.table)
library(dbplyr)
library(RPostgreSQL)

KrakenBillerProductAxiataDompulHD50<-readRDS(file="KrakenBillerProductAxiataDompulHD50.Rds")
KrakenBillerProductAxiataDompulD50<-readRDS(file="KrakenBillerProductAxiataDompulD50.Rds")
KrakenBillerProductAxiataDompulHD1<-readRDS(file="KrakenBillerProductAxiataDompulHD1.Rds")
KrakenBillerProductAxiataDompulD1<-readRDS(file="KrakenBillerProductAxiataDompulD1.Rds")

KrakenBillerProductAxiataDompulH<-rbind(KrakenBillerProductAxiataDompulHD50,KrakenBillerProductAxiataDompulHD1)
KrakenBillerProductAxiataDompul<-rbind(KrakenBillerProductAxiataDompulD50,KrakenBillerProductAxiataDompulD1)

#KrakenBillerProductAxiataDompulH<-KrakenBillerProductAxiataDompulHD50
#KrakenBillerProductAxiataDompul<-KrakenBillerProductAxiataDompulD50

#subset data for last 50 days
KrakenBillerProductAxiataDompulH<-KrakenBillerProductAxiataDompulH%>%filter(
  transactiondate>=max(transactiondate)-49 & transactiondate<=max(transactiondate))
KrakenBillerProductAxiataDompul<-KrakenBillerProductAxiataDompul%>%filter(
  transactiondate>=max(transactiondate)-49 & transactiondate<=max(transactiondate))
saveRDS(KrakenBillerProductAxiataDompulH,file="/home/rstudio/KrakenBillerProductAxiataDompulHD50.Rds",compress=F)
saveRDS(KrakenBillerProductAxiataDompul,file="/home/rstudio/KrakenBillerProductAxiataDompulD50.Rds",compress=F)

KrakenBillerProductAxiataDompulHo<-KrakenBillerProductAxiataDompulH

#1. Form dompul code with dompul name 

krkcon <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                         host = "airavata.cn26rqn0gh39.ap-southeast-1.rds.amazonaws.com",   
                         port = 5432,   
                         dbname = "sepulsa_reporting",   
                         user = "eko",   
                         password = "p3Payag4nTUn6" )

dompulCodeQuery<-dbSendQuery(krkcon,
                             "select code as dompulCode,
       label as partnerName,
       biller as biller
from internal.dompul")
dompulCode<-as.data.frame(dbFetch(dompulCodeQuery))
dbDisconnect(krkcon)

colnames(dompulCode)<-c("dompul code","dompul name","biller")
dompulCode$billermessage<-dompulCode$`dompul code`

dompulCode<-dompulCode[,c(1,3,2,4)]
dompulCode$`dompul code`<-as.character(dompulCode$`dompul code`)
dompulCode$`dompul name`<-as.character(dompulCode$`dompul name`)
dompulCode$billermessage<-dompulCode$`dompul code`

#merge with high level & calculate proportion

KrakenBillerProductAxiataDompul<-merge(x=KrakenBillerProductAxiataDompul,y=KrakenBillerProductAxiataDompulH,by="transactiondate")
colnames(KrakenBillerProductAxiataDompul)[c(3,4)]<-c("trxValueD","trxValueT")
KrakenBillerProductAxiataDompul$propD<-round((KrakenBillerProductAxiataDompul$trxValueD/KrakenBillerProductAxiataDompul$trxValueT)*100,digits=4)


#2. Decompose dompul data
# 1) Axiata

Axiata4820<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='4820',]
Axiata7214<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='7214',]
Axiata7215<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='7215',]
Axiata7217<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='7217',]
Axiata7222<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='7222',]
Axiata9531<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9531',]
Axiata9559<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9559',]
Axiata9572<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9572',]
Axiata9575<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9575',]
Axiata9578<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9578',]
Axiata9579<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9579',]
#Axiata9580<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9580',]
Axiata9631<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9631',]
Axiata9671<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9671',]
Axiata9676<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9676',]
Axiata9710<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9710',]
Axiata9712<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9712',]
Axiata9713<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9713',]
Axiata9714<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9714',]
#Axiata9742<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9742',]
Axiata9748<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9748',]
Axiata9754<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9754',]
Axiata9757<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9757',]
Axiata9760<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9760',]
Axiata9775<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9775',]
#Axiata9776<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9776',]
Axiata9777<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9777',]
Axiata9995<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9995',]
#Axiata9749<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9749',]
#Axiata9575<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9575',]
#Axiata9678<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9678',]
#Axiata9744<-KrakenBillerProductAxiataDompul[KrakenBillerProductAxiataDompul$billermessage=='9744',]


# 3. Add dompul name in the dataset
# Axiata

#Axiata4820<-merge(x=Axiata4820,y=dompulCode,by="billermessage")
Axiata7214<-merge(x=Axiata7214,y=dompulCode,by="billermessage")
Axiata7215<-merge(x=Axiata7215,y=dompulCode,by="billermessage")
Axiata7217<-merge(x=Axiata7217,y=dompulCode,by="billermessage")
Axiata7222<-merge(x=Axiata7222,y=dompulCode,by="billermessage")
Axiata9531<-merge(x=Axiata9531,y=dompulCode,by="billermessage")
Axiata9559<-merge(x=Axiata9559,y=dompulCode,by="billermessage")
Axiata9572<-merge(x=Axiata9572,y=dompulCode,by="billermessage")
Axiata9575<-merge(x=Axiata9575,y=dompulCode,by="billermessage")
Axiata9578<-merge(x=Axiata9578,y=dompulCode,by="billermessage")
Axiata9579<-merge(x=Axiata9579,y=dompulCode,by="billermessage")
#Axiata9580<-merge(x=Axiata9580,y=dompulCode,by="billermessage")
Axiata9631<-merge(x=Axiata9631,y=dompulCode,by="billermessage")
Axiata9671<-merge(x=Axiata9671,y=dompulCode,by="billermessage")
Axiata9676<-merge(x=Axiata9676,y=dompulCode,by="billermessage")
Axiata9710<-merge(x=Axiata9710,y=dompulCode,by="billermessage")
Axiata9712<-merge(x=Axiata9712,y=dompulCode,by="billermessage")
Axiata9713<-merge(x=Axiata9713,y=dompulCode,by="billermessage")
Axiata9714<-merge(x=Axiata9714,y=dompulCode,by="billermessage")
Axiata9748<-merge(x=Axiata9748,y=dompulCode,by="billermessage")
Axiata9754<-merge(x=Axiata9754,y=dompulCode,by="billermessage")
Axiata9757<-merge(x=Axiata9757,y=dompulCode,by="billermessage")
Axiata9760<-merge(x=Axiata9760,y=dompulCode,by="billermessage")
Axiata9775<-merge(x=Axiata9775,y=dompulCode,by="billermessage")
#Axiata9776<-merge(x=Axiata9776,y=dompulCode,by="billermessage")
Axiata9777<-merge(x=Axiata9777,y=dompulCode,by="billermessage")
Axiata9995<-merge(x=Axiata9995,y=dompulCode,by="billermessage")


#4. Data cleansing - outlier handling

##i.define lower limit

l7214<-nrow(Axiata7214)-6
l7215<-nrow(Axiata7215)-6
l7217<-nrow(Axiata7217)-6
l7222<-nrow(Axiata7222)-6
l9531<-nrow(Axiata9531)-6
l9572<-nrow(Axiata9572)-6
l9575<-nrow(Axiata9575)-6
l9578<-nrow(Axiata9578)-6
l9579<-nrow(Axiata9579)-6
l9631<-nrow(Axiata9631)-3
l9671<-nrow(Axiata9671)-3
l9676<-nrow(Axiata9676)-3
l9710<-nrow(Axiata9710)-6
l9712<-nrow(Axiata9712)-6
l9713<-nrow(Axiata9713)-6
l9714<-nrow(Axiata9714)-6
l9748<-nrow(Axiata9748)-6
l9754<-nrow(Axiata9754)-6
l9757<-nrow(Axiata9757)-6
l9760<-nrow(Axiata9760)-18
l9775<-nrow(Axiata9775)-6
#l9776<-nrow(Axiata9776)-1
l9777<-nrow(Axiata9777)-6
l9995<-nrow(Axiata9995)-6


#ii.Data cleansing - outlier handling

Axiata7214$propD[Axiata7214$propD<mean(Axiata7214[c(l7214:nrow(Axiata7214)),5]) | Axiata7214$propD>mean(Axiata7214[c(l7214:nrow(Axiata7214)),5])]<-mean(Axiata7214[c(l7214:nrow(Axiata7214)),5])*1.1

Axiata7215$propD[Axiata7215$propD<mean(Axiata7215[c(l7215:nrow(Axiata7215)),5]) | Axiata7215$propD>mean(Axiata7215[c(l7215:nrow(Axiata7215)),5])]<-mean(Axiata7215[c(l7215:nrow(Axiata7215)),5])*1.1

Axiata7217$propD[Axiata7217$propD<mean(Axiata7217[c(l7217:nrow(Axiata7217)),5]) | Axiata7217$propD>mean(Axiata7217[c(l7217:nrow(Axiata7217)),5])]<-mean(Axiata7217[c(l7217:nrow(Axiata7217)),5])*1.25

Axiata7222$propD[Axiata7222$propD<mean(Axiata7222[c(l7222:nrow(Axiata7222)),5]) | Axiata7222$propD>mean(Axiata7222[c(l7222:nrow(Axiata7222)),5])]<-mean(Axiata7222[c(l7222:nrow(Axiata7222)),5])*1.25

Axiata9531$propD[Axiata9531$propD<mean(Axiata9531[c(l9531:nrow(Axiata9531)),5])]<-mean(Axiata9531[c(l9531:nrow(Axiata9531)),5])*1.15

Axiata9572$propD[Axiata9572$propD<mean(Axiata9572[c(l9572:nrow(Axiata9572)),5]) | Axiata9572$propD>mean(Axiata9572[c(l9572:nrow(Axiata9572)),5])]<-mean(Axiata9572[c(l9572:nrow(Axiata9572)),5])*1.15

Axiata9575$propD[Axiata9575$propD<mean(Axiata9575[c(l9575:nrow(Axiata9575)),5]) | Axiata9575$propD>mean(Axiata9575[c(l9575:nrow(Axiata9575)),5])]<-mean(Axiata9575[c(l9575:nrow(Axiata9575)),5])*1.1

Axiata9578$propD[Axiata9578$propD>mean(Axiata9578[c(l9578:nrow(Axiata9578)),5]) | Axiata9578$propD<mean(Axiata9578[c(l9578:nrow(Axiata9578)),5])]<-mean(Axiata9578[c(l9578:nrow(Axiata9578)),5])*1.1

Axiata9579$propD[Axiata9579$propD<mean(Axiata9579[c(l9579:nrow(Axiata9579)),5]) | Axiata9579$propD>mean(Axiata9579[c(l9579:nrow(Axiata9579)),5])]<-mean(Axiata9579[c(l9579:nrow(Axiata9579)),5])*1.1

Axiata9631$propD[Axiata9631$propD<mean(Axiata9631[c(l9631:nrow(Axiata9631)),5]) | Axiata9631$propD>mean(Axiata9631[c(l9631:nrow(Axiata9631)),5])]<-mean(Axiata9631[c(l9631:nrow(Axiata9631)),5])*1.1

Axiata9671$propD[Axiata9671$propD<mean(Axiata9671[c((l9671-3):nrow(Axiata9671)),5]) | Axiata9671$propD>mean(Axiata9671[c((l9671-3):nrow(Axiata9671)),5])]<-mean(Axiata9671[c(l9671:nrow(Axiata9671)),5])*1.05

Axiata9676$propD[Axiata9676$propD<mean(Axiata9676[c((l9676-3):nrow(Axiata9676)),5]) | Axiata9676$propD>mean(Axiata9676[c((l9676-3):nrow(Axiata9676)),5])]<-mean(Axiata9676[c(l9676:nrow(Axiata9676)),5])*1.3

Axiata9710$propD[Axiata9710$propD>mean(Axiata9710[c(l9710:nrow(Axiata9710)),5]) | Axiata9710$propD<mean(Axiata9710[c(l9710:nrow(Axiata9710)),5])]<-mean(Axiata9710[c(l9710:nrow(Axiata9710)),5])*1.2

Axiata9712$propD[Axiata9712$propD<mean(Axiata9712[c(l9712:nrow(Axiata9712)),5]) | Axiata9712$propD>mean(Axiata9712[c(l9712:nrow(Axiata9712)),5])]<-mean(Axiata9712[c(l9712:nrow(Axiata9712)),5])*1.16

Axiata9713$propD[Axiata9713$propD<mean(Axiata9713[c(l9713:nrow(Axiata9713)),5]) | Axiata9713$propD>mean(Axiata9713[c(l9713:nrow(Axiata9713)),5])]<-mean(Axiata9713[c(l9713:nrow(Axiata9713)),5])*1.35

Axiata9714$propD[Axiata9714$propD<mean(Axiata9714[c(l9714:nrow(Axiata9714)),5]) | Axiata9714$propD>mean(Axiata9714[c(l9714:nrow(Axiata9714)),5])]<-mean(Axiata9714[c(l9714:nrow(Axiata9714)),5])*1.25

Axiata9748$propD[Axiata9748$propD<mean(Axiata9748[c(l9748:nrow(Axiata9748)),5]) | Axiata9748$propD>mean(Axiata9748[c(l9748:nrow(Axiata9748)),5])]<-mean(Axiata9748[c(l9748:nrow(Axiata9748)),5])*1.4

Axiata9754$propD[Axiata9754$propD<mean(Axiata9754[c(l9754:nrow(Axiata9754)),5]) | Axiata9754$propD>mean(Axiata9754[c(l9754:nrow(Axiata9754)),5])]<-mean(Axiata9754[c(l9754:nrow(Axiata9754)),5])*1.1

Axiata9757$propD[Axiata9757$propD<mean(Axiata9757[c(l9757:nrow(Axiata9757)),5]) | Axiata9757$propD>mean(Axiata9757[c(l9757:nrow(Axiata9757)),5])]<-mean(Axiata9757[c(l9757:nrow(Axiata9757)),5])*1.1

Axiata9760$propD[Axiata9760$propD<mean(Axiata9760[c(l9760:nrow(Axiata9760)),5]) | 
                   Axiata9760$propD>mean(Axiata9760[c(l9760:nrow(Axiata9760)),5])]<-mean(Axiata9760[c(l9760:nrow(Axiata9760)),5])*1.1

Axiata9775$propD[Axiata9775$propD>mean(Axiata9775[c(l9775:nrow(Axiata9775)),5]) |
                   Axiata9775$propD<mean(Axiata9775[c(l9775:nrow(Axiata9775)),5])]<-mean(Axiata9775[c(l9775:nrow(Axiata9775)),5])*1.35

#Axiata9776$propD[Axiata9776$propD>mean(Axiata9776[c(l9776:nrow(Axiata9776)),5]) |
#Axiata9776$propD<mean(Axiata9776[c(l9776:nrow(Axiata9776)),5])]<-mean(Axiata9776[c(l9776:nrow(Axiata9776)),5])*1.1

Axiata9777$propD[Axiata9777$propD>mean(Axiata9777[c(l9777:nrow(Axiata9777)),5]) |
                   Axiata9777$propD<mean(Axiata9777[c(l9777:nrow(Axiata9777)),5])]<-mean(Axiata9777[c(l9777:nrow(Axiata9777)),5])*1.35

Axiata9995$propD[Axiata9995$propD>mean(Axiata9995[c(l9995:nrow(Axiata9995)),5]) |
                   Axiata9995$propD<mean(Axiata9995[c(l9995:nrow(Axiata9995)),5])]<-mean(Axiata9995[c(l9995:nrow(Axiata9995)),5])*1.05



#5. Forecast for complete dompul dataset (to improve for the incomplete dompul dataset add missing value replacement)

# Form timeseries dataset

# Complete Axiata timeseries

#Axiata4820ts<-ts(Axiata4820$trxValue,start=range(Axiata4820$date)[1],end=range(Axiata4820$date)[2],frequency=30)
Axiata7214ts<-ts(Axiata7214$propD,start=range(Axiata7214$transactiondate)[1],end=range(Axiata7214$transactiondate)[2],frequency=30)
Axiata7215ts<-ts(Axiata7215$propD,start=range(Axiata7215$transactiondate)[1],end=range(Axiata7215$transactiondate)[2],frequency=30)
Axiata7217ts<-ts(Axiata7217$propD,start=range(Axiata7217$transactiondate)[1],end=range(Axiata7217$transactiondate)[2],frequency=30)
Axiata7222ts<-ts(Axiata7222$propD,start=range(Axiata7222$transactiondate)[1],end=range(Axiata7222$transactiondate)[2],frequency=30)
Axiata9531ts<-ts(Axiata9531$propD,start=range(Axiata9531$transactiondate)[1],end=range(Axiata9531$transactiondate)[2],frequency=30)
#Axiata9559ts<-ts(Axiata9559$trxValue,start=range(Axiata9559$transactiondate)[1],end=range(Axiata9559$transactiondate)[2],frequency=30)
Axiata9572ts<-ts(Axiata9572$propD,start=range(Axiata9572$transactiondate)[1],end=range(Axiata9572$transactiondate)[2],frequency=30)
Axiata9575ts<-ts(Axiata9575$propD,start=range(Axiata9575$transactiondate)[1],end=range(Axiata9575$transactiondate)[2],frequency=30)
Axiata9578ts<-ts(Axiata9578$propD,start=range(Axiata9578$transactiondate)[1],end=range(Axiata9578$transactiondate)[2],frequency=30)
Axiata9579ts<-ts(Axiata9579$propD,start=range(Axiata9579$transactiondate)[1],end=range(Axiata9579$transactiondate)[2],frequency=30)
#Axiata9580ts<-ts(Axiata9580$trxValue,start=range(Axiata9580$transactiondate)[1],end=range(Axiata9580$transactiondate)[2],frequency=30)
Axiata9631ts<-ts(Axiata9631$propD,start=range(Axiata9631$transactiondate)[1],end=range(Axiata9631$transactiondate)[2],frequency=30)
Axiata9671ts<-ts(Axiata9671$propD,start=range(Axiata9671$transactiondate)[1],end=range(Axiata9671$transactiondate)[2],frequency=30)
Axiata9676ts<-ts(Axiata9676$propD,start=range(Axiata9676$transactiondate)[1],end=range(Axiata9676$transactiondate)[2],frequency=30)
Axiata9710ts<-ts(Axiata9710$propD,start=range(Axiata9710$transactiondate)[1],end=range(Axiata9710$transactiondate)[2],frequency=30)
Axiata9712ts<-ts(Axiata9712$propD,start=range(Axiata9712$transactiondate)[1],end=range(Axiata9712$transactiondate)[2],frequency=30)
Axiata9713ts<-ts(Axiata9713$propD,start=range(Axiata9713$transactiondate)[1],end=range(Axiata9713$transactiondate)[2],frequency=30)
Axiata9714ts<-ts(Axiata9714$propD,start=range(Axiata9714$transactiondate)[1],end=range(Axiata9714$transactiondate)[2],frequency=30)
Axiata9748ts<-ts(Axiata9748$propD,start=range(Axiata9748$transactiondate)[1],end=range(Axiata9748$transactiondate)[2],frequency=30)
Axiata9754ts<-ts(Axiata9754$propD,start=range(Axiata9754$transactiondate)[1],end=range(Axiata9754$transactiondate)[2],frequency=30)
Axiata9757ts<-ts(Axiata9757$propD,start=range(Axiata9757$transactiondate)[1],end=range(Axiata9757$transactiondate)[2],frequency=30)
Axiata9760ts<-ts(Axiata9760$propD,start=range(Axiata9760$transactiondate)[1],end=range(Axiata9760$transactiondate)[2],frequency=30)
Axiata9775ts<-ts(Axiata9775$propD,start=range(Axiata9775$transactiondate)[1],end=range(Axiata9775$transactiondate)[2],frequency=30)
#Axiata9776ts<-ts(Axiata9776$propD,start=range(Axiata9776$transactiondate)[1],end=range(Axiata9776$transactiondate)[2],frequency=30)
Axiata9777ts<-ts(Axiata9777$propD,start=range(Axiata9777$transactiondate)[1],end=range(Axiata9777$transactiondate)[2],frequency=30)
Axiata9995ts<-ts(Axiata9995$propD,start=range(Axiata9995$transactiondate)[1],end=range(Axiata9995$transactiondate)[2],frequency=30)



library(curl)
library(forecast)
#Axiata4820ts<-tsclean(Axiata4820ts)
Axiata7214ts<-tsclean(Axiata7214ts)
Axiata7215ts<-tsclean(Axiata7215ts)
Axiata7217ts<-tsclean(Axiata7217ts)
Axiata7222ts<-tsclean(Axiata7222ts)
Axiata9531ts<-tsclean(Axiata9531ts)
Axiata9572ts<-tsclean(Axiata9572ts)
Axiata9575ts<-tsclean(Axiata9575ts)
Axiata9578ts<-tsclean(Axiata9578ts)
Axiata9579ts<-tsclean(Axiata9579ts)
#Axiata9580ts<-tsclean(Axiata9580ts)
Axiata9631ts<-tsclean(Axiata9631ts)
Axiata9671ts<-tsclean(Axiata9671ts)
Axiata9676ts<-tsclean(Axiata9676ts)
Axiata9710ts<-tsclean(Axiata9710ts)
Axiata9712ts<-tsclean(Axiata9712ts)
Axiata9713ts<-tsclean(Axiata9713ts)
Axiata9714ts<-tsclean(Axiata9714ts)
Axiata9748ts<-tsclean(Axiata9748ts)
Axiata9754ts<-tsclean(Axiata9754ts)
Axiata9757ts<-tsclean(Axiata9757ts)
Axiata9760ts<-tsclean(Axiata9760ts)
Axiata9775ts<-tsclean(Axiata9775ts)
#Axiata9776ts<-tsclean(Axiata9776ts)
Axiata9777ts<-tsclean(Axiata9777ts)
Axiata9995ts<-tsclean(Axiata9995ts)



#6. Forecast
# Axiata

library(TTR)
library(forecast)
#Axiata4820SMA2<-HoltWinters(log(Axiata4820ts))
Axiata7214SMA2<-HoltWinters(Axiata7214ts,alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata7215SMA2<-HoltWinters(log(Axiata7215ts),alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata7217SMA2<-HoltWinters(Axiata7217ts,alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata7222SMA2<-HoltWinters(log(Axiata7222ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9531SMA2<-HoltWinters(log(Axiata9531ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9572SMA2<-HoltWinters(log(Axiata9572ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9575SMA2<-HoltWinters(log(Axiata9575ts),alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9578SMA2<-HoltWinters(log(Axiata9578ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="additive",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9579SMA2<-HoltWinters(log(Axiata9579ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

#Axiata9580SMA2<-HoltWinters(log(Axiata9580ts))

Axiata9631SMA2<-HoltWinters(log(Axiata9631ts),alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.4, gamma = 0.1),optim.control=list())

Axiata9671SMA2<-HoltWinters(log(Axiata9671ts),alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9676SMA2<-HoltWinters(log(Axiata9676ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9710SMA2<-HoltWinters(log(Axiata9710ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9712SMA2<-HoltWinters(log(Axiata9712ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9713SMA2<-HoltWinters(Axiata9713ts,alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.4, beta = 0.1, gamma = 0.6),optim.control=list())

Axiata9714SMA2<-HoltWinters(log(Axiata9714ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9748SMA2<-HoltWinters(log(Axiata9748ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.3, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9754SMA2<-HoltWinters(log(Axiata9754ts),alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9757SMA2<-HoltWinters(log(Axiata9757ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.4, gamma = 0.1),optim.control=list())

Axiata9760SMA2<-HoltWinters(Axiata9760ts,alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9775SMA2<-HoltWinters(log(Axiata9775ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

#Axiata9776SMA2<-HoltWinters(Axiata9776ts,alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9777SMA2<-HoltWinters(log(Axiata9777ts),alpha=NULL,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())

Axiata9995SMA2<-HoltWinters(log(Axiata9995ts),alpha=TRUE,gamma=TRUE)

# Prediction 
# Axiata 
# t=7

#Axiata4820F<-forecast(Axiata4820SMA2,h=3)
Axiata7214F<-forecast(Axiata7214SMA2,h=8)
Axiata7215F<-forecast(Axiata7215SMA2,h=8)
Axiata7217F<-forecast(Axiata7217SMA2,h=8)
Axiata7222F<-forecast(Axiata7222SMA2,h=8)
Axiata9531F<-forecast(Axiata9531SMA2,h=8)
Axiata9572F<-forecast(Axiata9572SMA2,h=8)
Axiata9575F<-forecast(Axiata9575SMA2,h=8)
Axiata9578F<-forecast(Axiata9578SMA2,h=8)
Axiata9579F<-forecast(Axiata9579SMA2,h=8)
#Axiata9580F<-forecast(Axiata9580SMA2,h=3)
Axiata9631F<-forecast(Axiata9631SMA2,h=8)
Axiata9671F<-forecast(Axiata9671SMA2,h=8)
Axiata9676F<-forecast(Axiata9676SMA2,h=8)
Axiata9710F<-forecast(Axiata9710SMA2,h=8)
Axiata9712F<-forecast(Axiata9712SMA2,h=8)
Axiata9713F<-forecast(Axiata9713SMA2,h=8)
Axiata9714F<-forecast(Axiata9714SMA2,h=8)
Axiata9748F<-forecast(Axiata9748SMA2,h=8)
Axiata9754F<-forecast(Axiata9754SMA2,h=8)
Axiata9757F<-forecast(Axiata9757SMA2,h=8)
Axiata9760F<-forecast(Axiata9760SMA2,h=8)
Axiata9775F<-forecast(Axiata9775SMA2,h=8)
#Axiata9776F<-forecast(Axiata9776SMA2,h=8)
Axiata9777F<-forecast(Axiata9777SMA2,h=8)
Axiata9995F<-forecast(Axiata9995SMA2,h=8)



# Form forecast table
# Axiata
#solution to convert data frame from list getting error

Axiata7214DF<-as.data.frame(Axiata7214F$mean)
Axiata7214DF$x<-as.numeric(as.character(round(Axiata7214DF$x,4)))
Axiata7215DF<-as.data.frame(Axiata7215F$mean)
Axiata7215DF$x<-as.numeric(as.character(round(exp(Axiata7215DF$x),4)))
Axiata7217DF<-as.data.frame(Axiata7217F$mean)
Axiata7217DF$x<-as.numeric(as.character(round(Axiata7217DF$x,4)))
Axiata7222DF<-as.data.frame(Axiata7222F$mean)
Axiata7222DF$x<-as.numeric(as.character(round(exp(Axiata7222DF$x),4)))
Axiata9531DF<-as.data.frame(Axiata9531F$mean)
Axiata9531DF$x<-as.numeric(as.character(round(exp(Axiata9531DF$x),4)))
Axiata9572DF<-as.data.frame(Axiata9572F$mean)
Axiata9572DF$x<-as.numeric(as.character(round(exp(Axiata9572DF$x),4)))
Axiata9575DF<-as.data.frame(Axiata9575F$mean)
Axiata9575DF$x<-as.numeric(as.character(round(exp(Axiata9575DF$x),4)))
Axiata9578DF<-as.data.frame(Axiata9578F$mean)
Axiata9578DF$x<-as.numeric(as.character(round(exp(Axiata9578DF$x),4)))
Axiata9579DF<-as.data.frame(Axiata9579F$mean)
Axiata9579DF$x<-as.numeric(as.character(round(exp(Axiata9579DF$x),4)))
Axiata9631DF<-as.data.frame(Axiata9631F$mean)
Axiata9631DF$x<-as.numeric(as.character(round(exp(Axiata9631DF$x),4)))
Axiata9671DF<-as.data.frame(Axiata9671F$mean)
Axiata9671DF$x<-as.numeric(as.character(round(exp(Axiata9671DF$x),4)))
Axiata9676DF<-as.data.frame(Axiata9676F$mean)
Axiata9676DF$x<-as.numeric(as.character(round(exp(Axiata9676DF$x),4)))
Axiata9710DF<-as.data.frame(Axiata9710F$mean)
Axiata9710DF$x<-as.numeric(as.character(round(exp(Axiata9710DF$x),4)))
Axiata9712DF<-as.data.frame(Axiata9712F$mean)
Axiata9712DF$x<-as.numeric(as.character(round(exp(Axiata9712DF$x),4)))
Axiata9713DF<-as.data.frame(Axiata9713F$mean)
Axiata9713DF$x<-as.numeric(as.character(round(Axiata9713DF$x,4)))
Axiata9714DF<-as.data.frame(Axiata9714F$mean)
Axiata9714DF$x<-as.numeric(as.character(round(exp(Axiata9714DF$x),4)))
Axiata9748DF<-as.data.frame(Axiata9748F$mean)
Axiata9748DF$x<-as.numeric(as.character(round(exp(Axiata9748DF$x),4)))
Axiata9754DF<-as.data.frame(Axiata9754F$mean)
Axiata9754DF$x<-as.numeric(as.character(round(exp(Axiata9754DF$x),4)))
Axiata9757DF<-as.data.frame(Axiata9757F$mean)
Axiata9757DF$x<-as.numeric(as.character(round(exp(Axiata9757DF$x),4)))
Axiata9760DF<-as.data.frame(Axiata9760F$mean)
Axiata9760DF$x<-as.numeric(as.character(round(Axiata9760DF$x,4)))
Axiata9775DF<-as.data.frame(Axiata9775F$mean)
Axiata9775DF$x<-as.numeric(as.character(round(exp(Axiata9775DF$x),4)))
#Axiata9776DF<-as.data.frame(Axiata9776F$mean)
#Axiata9776DF$x<-as.numeric(as.character(round(Axiata9776DF$x,4)))
Axiata9777DF<-as.data.frame(Axiata9777F$mean)
Axiata9777DF$x<-as.numeric(as.character(round(exp(Axiata9777DF$x),4)))
Axiata9995DF<-as.data.frame(Axiata9995F$mean)
Axiata9995DF$x<-as.numeric(as.character(round(exp(Axiata9995DF$x),4)))



# 6. Form deposit schema dataset

#Axiata
AxiataADF<-as.data.frame(cbind(
  as.numeric(as.character(Axiata7214DF$x)),
  as.numeric(as.character(Axiata7215DF$x)),
  as.numeric(as.character(Axiata7217DF$x)),
  as.numeric(as.character(Axiata7222DF$x)),
  as.numeric(as.character(Axiata9531DF$x)),
  as.numeric(as.character(Axiata9572DF$x)),
  as.numeric(as.character(Axiata9575DF$x)),
  as.numeric(as.character(Axiata9578DF$x)),
  as.numeric(as.character(Axiata9579DF$x)),
  #as.numeric(as.character(Axiata9580DF$x)),
  as.numeric(as.character(Axiata9631DF$x)),
  as.numeric(as.character(Axiata9671DF$x)),
  as.numeric(as.character(Axiata9676DF$x)),
  as.numeric(as.character(Axiata9710DF$x)),
  as.numeric(as.character(Axiata9712DF$x)),
  as.numeric(as.character(Axiata9713DF$x)),
  as.numeric(as.character(Axiata9714DF$x)),
  as.numeric(as.character(Axiata9748DF$x)),
  as.numeric(as.character(Axiata9754DF$x)),
  as.numeric(as.character(Axiata9757DF$x)),
  as.numeric(as.character(Axiata9760DF$x)),
  as.numeric(as.character(Axiata9775DF$x)),
  #as.numeric(as.character(Axiata9776DF$x)),
  as.numeric(as.character(Axiata9777DF$x)),
  as.numeric(as.character(Axiata9995DF$x))))
#as.numeric(as.character(Axiata4820DF$x)),
AxiataADF$Total<-rowSums(AxiataADF)


#Rename the column & row

#Axiata

colnames(AxiataADF)[1:23]<-as.numeric(as.character(c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995")))


#Transpose the forecast dataframe

AxiataADFt<-as.data.frame(t(AxiataADF))
AxiataADFt$billermessage<-c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995","Total")
AxiataADFt<-merge(x=AxiataADFt,y=dompulCode[c(4,3)],by="billermessage",all.x=TRUE)
AxiataADFt<-AxiataADFt[,c(1,10,2:9)]


#7. high level forecast
KrakenBillerProductAxiataDompulH$trxValue[KrakenBillerProductAxiataDompulH$trxValue<mean(KrakenBillerProductAxiataDompulH[c(44:50),2])| KrakenBillerProductAxiataDompulH$trxValue>mean(KrakenBillerProductAxiataDompulH[c(44:50),2])]<-mean(KrakenBillerProductAxiataDompulH[c(44:50),2])*1.05
AxiataHts<-ts(KrakenBillerProductAxiataDompulH$trxValue,start=range(KrakenBillerProductAxiataDompulH$transactiondate)[1],end=range(KrakenBillerProductAxiataDompulH$transactiondate)[2],frequency=30)
AxiataHts<-tsclean(AxiataHts)
AxiataHSMA2<-HoltWinters(log(AxiataHts),alpha=TRUE,beta=NULL,gamma=TRUE,seasonal="multiplicative",l.start=NULL,b.start=NULL,s.start=NULL,optim.start=c(alpha = 0.2, beta = 0.1, gamma = 0.1),optim.control=list())
AxiataHF<-forecast(AxiataHSMA2,h=8)
AxiataHDF<-as.data.frame(AxiataHF$mean)
AxiataHDF$x<-as.numeric(as.character(round(exp(AxiataHDF$x),0)))


# Multiply high level with low level forecast

AxiataADFt$M1<-(AxiataADFt$V1/100)*AxiataHDF[1,]
AxiataADFt$M2<-(AxiataADFt$V2/100)*AxiataHDF[2,]
AxiataADFt$M3<-(AxiataADFt$V3/100)*AxiataHDF[3,]
AxiataADFt$M4<-(AxiataADFt$V4/100)*AxiataHDF[4,]
AxiataADFt$M5<-(AxiataADFt$V5/100)*AxiataHDF[5,]
AxiataADFt$M6<-(AxiataADFt$V6/100)*AxiataHDF[6,]
AxiataADFt$M7<-(AxiataADFt$V7/100)*AxiataHDF[7,]
AxiataADFt$M8<-(AxiataADFt$V8/100)*AxiataHDF[8,]

AxiataADFt<-AxiataADFt[,c(1:2,11:18)]

# Add projection date in table

currentDate<-as.Date(Sys.Date())
currentDate
Future0<-as.character(currentDate)
Future1<-as.character(currentDate+1)
Future2<-as.character(currentDate+2)
Future3<-as.character(currentDate+3)
Future4<-as.character(currentDate+4)
Future5<-as.character(currentDate+5)
Future6<-as.character(currentDate+6)
Future7<-as.character(currentDate+7)


# Add date to projection table

colnames(AxiataADFt)[3:10]<-c(Future0,Future1,Future2,Future3,Future4,Future5,Future6,Future7)


#Deviation
# dataset for deviation (last 4 weeks)
#StCvAxiata<-readRDS(file="StCvAxiata1.Rds")


# Form projection table
# biller,deposit,date,dompul code,dompul name


#form projection dataset, add date & formatting
#form projection dataset, add date & formatting
ProSch<-AxiataADFt
ProSch0<-as.data.frame(cbind(ProSch[c(1:23),3],"date"=Future0))
ProSch1<-as.data.frame(cbind(ProSch[c(1:23),4],"date"=Future1))
ProSch2<-as.data.frame(cbind(ProSch[c(1:23),5],"date"=Future2))
ProSch3<-as.data.frame(cbind(ProSch[c(1:23),6],"date"=Future3))
ProSch4<-as.data.frame(cbind(ProSch[c(1:23),7],"date"=Future4))
ProSch5<-as.data.frame(cbind(ProSch[c(1:23),8],"date"=Future5))
ProSch6<-as.data.frame(cbind(ProSch[c(1:23),9],"date"=Future6))
ProSch7<-as.data.frame(cbind(ProSch[c(1:23),10],"date"=Future7))
ProSch0$date<-as.Date(ProSch0$date)
ProSch1$date<-as.Date(ProSch1$date)
ProSch2$date<-as.Date(ProSch2$date)
ProSch3$date<-as.Date(ProSch3$date)
ProSch4$date<-as.Date(ProSch4$date)
ProSch5$date<-as.Date(ProSch5$date)
ProSch6$date<-as.Date(ProSch6$date)
ProSch7$date<-as.Date(ProSch7$date)

#binding dataset with dompul code
ProSch0<-cbind(ProSch0,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch0<-ProSch0[,c(3,1,2)]
ProSch1<-cbind(ProSch1,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch1<-ProSch1[,c(3,1,2)]
ProSch2<-cbind(ProSch2,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch2<-ProSch2[,c(3,1,2)]
ProSch3<-cbind(ProSch3,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch3<-ProSch3[,c(3,1,2)]
ProSch4<-cbind(ProSch4,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch4<-ProSch4[,c(3,1,2)]
ProSch5<-cbind(ProSch5,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch5<-ProSch5[,c(3,1,2)]
ProSch6<-cbind(ProSch6,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch6<-ProSch6[,c(3,1,2)]
ProSch7<-cbind(ProSch7,"dompul code"=c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9777","9995"))
ProSch7<-ProSch7[,c(3,1,2)]

#rename deposit column & convert into numeric
colnames(ProSch0)[2]<-"deposit"
colnames(ProSch1)[2]<-"deposit"
colnames(ProSch2)[2]<-"deposit"
colnames(ProSch3)[2]<-"deposit"
colnames(ProSch4)[2]<-"deposit"
colnames(ProSch5)[2]<-"deposit"
colnames(ProSch6)[2]<-"deposit"
colnames(ProSch7)[2]<-"deposit"
ProSch0$deposit<-as.numeric(as.character(ProSch0$deposit))
ProSch1$deposit<-as.numeric(as.character(ProSch1$deposit))
ProSch2$deposit<-as.numeric(as.character(ProSch2$deposit))
ProSch3$deposit<-as.numeric(as.character(ProSch3$deposit))
ProSch4$deposit<-as.numeric(as.character(ProSch4$deposit))
ProSch5$deposit<-as.numeric(as.character(ProSch5$deposit))
ProSch6$deposit<-as.numeric(as.character(ProSch6$deposit))
ProSch7$deposit<-as.numeric(as.character(ProSch7$deposit))

#binding dompul code & domoul name column into projection schema
ProSch<-rbind(ProSch1,ProSch2,ProSch3,ProSch4,ProSch5,ProSch6,ProSch7)
ProSch<-merge(x=ProSch,y=dompulCode[c(1:3)],by="dompul code",all.x=TRUE)

#rearrange the schema
ProSch<-ProSch[,c(4,2:3,1,5)]

ratio<-sum(ProSch$deposit)/sum(KrakenBillerProductAxiataDompulHo[c(44:50),2])
ratio

#Obtain deposit data

# Connection to Postgres
krkcon <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                         host = "airavata.cn26rqn0gh39.ap-southeast-1.rds.amazonaws.com",   
                         port = 5432,   
                         dbname = "sepulsa_reporting",   
                         user = "eko",   
                         password = "p3Payag4nTUn6" )

billerInventoryLatestQuery<-dbSendQuery(krkcon,";with cteRowNumber as (
    select biller as operator,partner_id as partnerid,dompul_code as billermessage,deposit as deposit,created_at as createdat,
           row_number() over(partition by dompul_code order by created_at desc) as RowNum
        from internal.inventory_biller
)
select operator,partnerid,billermessage,deposit,createdat
    from cteRowNumber
    where RowNum = 1")
billerInventory<-as.data.frame(dbFetch(billerInventoryLatestQuery))


dbDisconnect(krkcon)

billerInventory$date<-as.Date(billerInventory$createdat)
billerInventoryAxiata<-billerInventory%>%filter(operator=='axiata')
colnames(billerInventoryAxiata)[c(3,4)]<-c("dompul code","stock")

ProSch<-merge(x=ProSch,y=billerInventoryAxiata[c(3,4)],by="dompul code")


#promotion dataset
#promo<-read.delim(file="promotion2007.csv",sep=";",header=T)
#colnames(promo)<-c("dompul code","promotion")

#merge to projection data
#ProSch<-merge(x=ProSch,y=promo,by="dompul code",all.x=T)
#ProSch[is.na(ProSch)]<-0
#ProSch$deposit<-ProSch$deposit+ProSch$promotion
#ProSch<-ProSch[,c(1:6)]


#Add biller id & created timestamp

krkcon <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                         host = "airavata.cn26rqn0gh39.ap-southeast-1.rds.amazonaws.com",   
                         port = 5432,   
                         dbname = "sepulsa_reporting",   
                         user = "eko",   
                         password = "p3Payag4nTUn6" )

billerIDQuery<-dbSendQuery(krkcon,
                           "select name as biller,
       id as biller_id
from erp.billers")
billerID<-as.data.frame(dbFetch(billerIDQuery))


dbDisconnect(krkcon)
colnames(billerID)<-c("biller","biller ID")
dompulCode$billerID<-dompulCode$`dompul code`

ProSch<-merge(x=ProSch,y=billerID,by="biller")

#add created at time

ProSch$`created at`<-as.POSIXct(Sys.time(),"%Y-%m-%d %H:%M:%S")

#Insert projection data schema into database

##first time insert & create

#dbWriteTable(con,"billerDeposit",value=ProSch,overwrite=T,append=F,row.names=FALSE)


#obtain latest id to be inserted to new records


con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = "postgres",   
                      password = "ap3Lmerahm4ni5" )

maxIdQuery<-dbSendQuery(con,"select max(id) from public.billerdeposit")

maxId<-as.data.frame(dbFetch(maxIdQuery))

ids<-maxId$max+1
ide<-maxId$max+nrow(ProSch)
ide-ids



#insert id to projection database


ProSch$id<-seq(ids, ide,by=1)


db_insert_into(con,"billerdeposit",value=ProSch)
dbDisconnect(con)



####################

#create weekly transaction average table

#i. create daily average demand table

#obtain minimum deposit date

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = "postgres",   
                      password = "ap3Lmerahm4ni5" )

minDateQuery<-dbSendQuery(con,"select min(date) as min, max(date) as max from public.billerdeposit")

minDepositDate<-as.data.frame(dbFetch(minDateQuery))

#get historical data, starting 1 week before first record of data

#define dataset 1 week before deposit

telcoHist<-KrakenBillerProductAxiataDompul%>%filter(transactiondate>=minDepositDate$max-14 & transactiondate<=minDepositDate$max-8)
#telcoHist<-filter(KrakenBillerProductH,transactiondate>=max(transactiondate)-6 & transactiondate<=max(transactiondate))



#add week number & year

telcoHist$week<-as.numeric(as.character(format(telcoHist$transactiondate,"%V")))
telcoHist$year<-as.numeric(as.character(format(telcoHist$transactiondate,"%Y")))


#filter specific telco

#telcoHistDAxiata<-filter(telcoHist,productoperator %in% c("xl","axis"))


#filter specific dompul

#telcoHistDAxiata<-filter(telcoHistDAxiata,billermessage%in%c("7214","7215","7217","7222","9531","9572","9575","9578","9579","9631","9671","9676","9710","9712","9713","9714","9748","9754","9757","9760","9775","9776","9777","9995"))


#summarise data by week & dompul

telcoHistDAxiata<-as.data.frame(telcoHist%>%group_by(week,year,billermessage)%>%summarize(trxValue=round(sum(trxValueD),2)))

#calculate the daily average 

telcoHistDAxiata$`daily average`<-round(telcoHistDAxiata$trxValue/7,0)


#add id 

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = "postgres",   
                      password = "ap3Lmerahm4ni5" )

maxIdHQuery<-dbSendQuery(con,"select max(id) from public.transactionhistory")

maxIdH<-as.data.frame(dbFetch(maxIdHQuery))

idsH<-maxIdH$max+1
ideH<-maxIdH$max+nrow(telcoHistDAxiata)
ideH-idsH

telcoHistDAxiata$id<-seq(idsH,ideH,by=1)

telcoHistDAxiata$id<-seq(idsH,ideH,by=1)

telcoHistDAxiata<-telcoHistDAxiata[,c(6,2,1,3,4,5)]


##Insert & create data table for the daily average demand on database (first time)

con <- DBI::dbConnect(RPostgreSQL::PostgreSQL(),    
                      host = "trxhourly.cmqarwvv5fja.us-east-1.rds.amazonaws.com",   
                      port = 5432,   
                      dbname = "postgres",   
                      user = "postgres",   
                      password = "ap3Lmerahm4ni5" )

#dbWriteTable(con,"transactionhistory",value=telcoHistDAxiata,overwrite=T,append=F,row.names=FALSE)
db_insert_into(con,"transactionhistory",value=telcoHistDAxiata)

dbDisconnect(con)


#Create email dataframe
#i. read dompul mapping dataset
dompulMap<-read.delim(file="dompulMapping.csv",sep=";",header=T)
colnames(dompulMap)[1]<-"dompul code"

#ii.transform projection data

AxiataProj<-ProSch%>%group_by(`dompul code`)%>%summarize(Propose=sum(deposit))
AxiataProj<-merge(x=AxiataProj,y=billerInventoryAxiata[c(3,4)],by="dompul code")
AxiataProj<-merge(x=AxiataProj,y=dompulMap[c(1,3:7)],by="dompul code")
AxiataProj$Propose<-as.numeric(as.character(AxiataProj$Propose-AxiataProj$stock))
AxiataProj<-AxiataProj[c(23,13,8,14,9,15,11,16,17,10,18,1,12,20,19,7,2,6,5,22,3,4,21),]
AxiataProj<-subset(AxiataProj,AxiataProj$Propose>0)
AxiataProj<-AxiataProj[,c(4:8,2:3)]
colnames(AxiataProj)<-c("ID Dealer (SAP)","No Dompul","Account Code","Dealer Name","Email","Propose","Balance")
AxiataProjR<-as.data.frame(c("","","Total","","",colSums(AxiataProj[,c(6:7)])))
colnames(AxiataProjR)<-"T"
AxiataProjR<-as.data.frame(t(AxiataProjR))
colnames(AxiataProjR)<-c("ID Dealer (SAP)","No Dompul","Account Code","Dealer Name","Email","Propose","Balance")
AxiataProj<-rbind(AxiataProj,AxiataProjR)
AxiataProj$Propose<-as.numeric(as.character(AxiataProj$Propose))
AxiataProj$Balance<-as.numeric(as.character(AxiataProj$Balance))
AxiataProj$Propose<-round(AxiataProj$Propose/10^5,0)*10^5
AxiataProj$`No Dompul`<-format(AxiataProj$`No Dompul`,scientific=F)
AxiataProj$Balance<-format(AxiataProj$Balance,big.mark=",")
AxiataProj$Propose<-format(AxiataProj$Propose,scientific=F,big.mark=",")

#create email body

library(xtable)
library(tableHTML)
mailDatasetDP=tableHTML::tableHTML(AxiataProj)
mailBody<-paste0("<p> Dear All, <p>",
                 "<p> 
                                <p>",
                 "<p> Please find below proposed deposit value for Axiata biller, <p>", 
                 "<p> 
                                    <p>",
                 mailDatasetDP,
                 "<p> 
                                    <p>",
                 ratio,
                 "<p> 
                                    <p>",
                 "<p> Regards <p>",
                 "<p> 
                                    <p>",
                 "<p> 
                                    <p>")


subjectAlp<-paste("Telco Deposit Proposed Mail", as.Date(max(billerInventoryAxiata$date))+1,sep=' ')

#send the mail

library(rJava)
library(mailR)
sender <- "eko.unitedindo@gmail.com"
recipients <- "kristianus@alterra.id"
send.mail(from = sender,
          to = recipients,
          subject = subjectAlp,
          body = mailBody,
          html=TRUE,
          smtp = list(host.name = "email-smtp.us-east-1.amazonaws.com", port = 25,
                      user.name = "AKIAVYWW3Q3J5DH3SSXI",
                      passwd = "BMs66BZPeH0eTGl2igs2xL4dUMZ/gMem7gTJel1akphh", ssl = TRUE),
          authenticate = TRUE,
          send = TRUE)

endTime<-Sys.time()
procTime<-endTime-startTime
procTime
