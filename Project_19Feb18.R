library(RJDBC)
library(rJava)
library(stringr)
library(lubridate)
library(dplyr)

subQuery <- function(accno)
{
  subscriptions <- dbGetQuery(conn,sprintf("SELECT
                                           rsds_ops_renewals.tbl_subscriptions.acc_num,
                                           rsds_ops_renewals.tbl_subscriptions.sku_num,
                                           tbl_product_msrp.sku_desc,
                                           rsds_ops_renewals.tbl_subscriptions.sku_qty,
                                           rsds_ops_renewals.tbl_subscriptions.service_start_date,
                                           rsds_ops_renewals.tbl_subscriptions.service_end_date
                                           FROM
                                           rsds_ops_renewals.tbl_subscriptions
                                           LEFT OUTER JOIN 
                                           rsds_ops_renewals.tbl_product_msrp
                                           ON 
                                           rsds_ops_renewals.tbl_subscriptions.sku_num = rsds_ops_renewals.tbl_product_msrp.sku_num
                                           WHERE
                                           rsds_ops_renewals.tbl_subscriptions.acc_num = %s", accno))
  return(subscriptions)
}

chkQuery <- function(accno)
{
  checkins <- dbGetQuery(conn,sprintf("SELECT
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.acc_num,
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.hostname,
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.system_id,
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.profile_create_date,
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.system_last_checkin_date,
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.consumer_type,
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.system_type
                                      FROM
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin
                                      WHERE
                                      rsds_ops_renewals.tbl_rhn_candlepin_checkin.acc_num = %s", accno))
  return(checkins)
}

CleanCheckin <- function(cl_chkin)
{
  d <- aggregate(cl_chkin$system_last_checkin_date,list(cl_chkin$acc_num,cl_chkin$hostname,cl_chkin$system_id,cl_chkin$profile_create_date),max)
  tmpChk2 <- merge(cl_chkin,d,by.x = c(1,2,3,4),by.y = c(1,2,3,4))
  tmpChk2[tmpChk2[,5]!=tmpChk2[,8],8] = NA
  tmpChk2 <- na.omit(tmpChk2)
  tmpChk2 <- within(tmpChk2,rm(x))
  return(tmpChk2)
}

LogReg <- function(LData)
{
  LData <- fd1[5:9]
  LData <- LData[-4]
  # LData <- finDataset[,c(5,6,8,9,11)]
  
  # Encoding the target feature as factor
  LData$DealSuccess <- factor(LData$DealSuccess, levels = c(0, 1))
  # LData$yum_mrepo_Spacewalk_cobbler <- factor(LData$yum_mrepo_Spacewalk_cobbler, levels = c(0, 1))
  
  # Splitting the dataset into the Training set and Test set
  library(caTools)
  split = sample.split(LData$DealSuccess, SplitRatio = 0.75)
  training_set = subset(LData, split == TRUE)
  test_set = subset(LData, split == FALSE)
  
  # Feature Scaling
  training_set[1:3] = scale(training_set[1:3])
  test_set[1:3] = scale(test_set[1:3])
  
  # Fitting Logistic Regression to the Training set
  classifier = glm(formula = DealSuccess ~ .,
                   family = binomial,
                   data = training_set)
  
  # Predicting the Test set results
  prob_pred = predict(classifier, type = 'response', newdata = test_set[1:3])
  y_pred = ifelse(prob_pred > 0.5, 1, 0)
  
  # Making the Confusion Matrix
  cm = table(test_set[,4], y_pred > 0.5)
}

DecisionTree <- function(ld)
{
  ld <- fd1[5:9]
  ld <- ld[-4]
  
  ld$DealSuccess <- factor(ld$DealSuccess, levels = c(0, 1))
  
  library(caTools)
  set.seed(123)
  split = sample.split(ld$DealSuccess, SplitRatio = 0.75)
  training_set = subset(ld, split == TRUE)
  test_set = subset(ld, split == FALSE)
  
  # Feature Scaling
  training_set[1:3] = scale(training_set[1:3])
  test_set[1:3] = scale(test_set[1:3])
  
  library(rpart)
  classifier = rpart(formula = DealSuccess ~ .,
                     data = training_set)
  
  y_pred = predict(classifier, newdata = test_set[-4], type = 'class')
  
  cm = table(test_set[, 4], y_pred)
}

RandomForest <- function(ld)
{
  ld <- fd1[5:9]
  ld <- ld[-4]
  
  ld$DealSuccess <- factor(ld$DealSuccess, levels = c(0, 1))
  
  library(caTools)
  set.seed(123)
  split = sample.split(ld$DealSuccess, SplitRatio = 0.75)
  training_set = subset(ld, split == TRUE)
  test_set = subset(ld, split == FALSE)
  
  training_set[1:3] = scale(training_set[1:3])
  test_set[1:3] = scale(test_set[1:3])
  
  library(randomForest)
  set.seed(123)
  classifier = randomForest(x = training_set[-4],
                            y = training_set$DealSuccess,
                            ntree = 500)
  
  y_pred = predict(classifier, newdata = test_set[-4])
  
  cm = table(test_set[, 4], y_pred)
}

CleanDataset <- function(DS)
{
  #Cleaning '*' from Account Number
  DS$Account <- stringr::str_replace(DS$EBS.Account.Number.s.,'\\*','') 
  
  #Cleaning $0 Opportunities
  DS <- subset(DS,DS$Amount..converted.!=0 | DS$Amount..converted.!="")
  
  #Cleaning duplicate -ve Opps
  DS$DeleteRow <- NA
  for(i in 1:nrow(DS))
  {
    if(any(DS[i,13]==-DS$Amount..converted. & DS[i,14]==DS$Account)) DS[i,15] <- 1 else DS[i,15] <- 0
  }
  DS <- subset(DS, DS$DeleteRow==0, select = -DeleteRow)
  
  #Converting NA Geo to character value
  levels <- levels(DS$Super.Region)
  levels[length(levels)+1] <- "NA"
  DS$Super.Region <- factor(DS$Super.Region,levels = levels)
  DS$Super.Region[is.na(DS$Super.Region)] <- "NA"
  
  #Adding SEAP Deal Y or N based on Closed Booked or Closed Lost
  index <- c("Closed","Omitted")
  values <- c(1,0)
  DS$DealSuccess <- values[match(DS$Forecast.Category,index)]
  
  #Rename Columns
  colnames(DS)[4] <- "Geo"
  
  return(DS)
}

multiplier <- function(sub)
{
  one1 <- one2 <- one3 <- one4 <- two <- four1 <- four2 <- four3 <- five <- ten <- fifteen <- sixteen1 <- sixteen2 <- sixteen3 <- sixteen4 <- sixteen5 <- fifty <- hundred <- fourhundred <- 0
  
  one1 <- ifelse(grepl("1 GUEST",toupper(sub$sku_desc)) | grepl("1 VIRTUAL MACHINE",toupper(sub$sku_desc)) == TRUE,sub$sku_qty,0)
  one2 <- ifelse(!grepl("DESKTOP, SELF-SUPPORT",toupper(sub$sku_desc)) &
                 !grepl("1 GUEST",toupper(sub$sku_desc)) &
                 !grepl("1 VIRTUAL MACHINE",toupper(sub$sku_desc)) &
                 !grepl("WORKSTATION",toupper(sub$sku_desc)) &
                 !grepl("UNLIMITED",toupper(sub$sku_desc)) &
                  grepl("SELF-SUPPORT",toupper(sub$sku_desc)) == TRUE,
                  sub$sku_qty,0)
  one3 <- ifelse(!grepl("DEVELOPER WORKSTATION, ENTERPRISE",toupper(sub$sku_desc)) &
                 !grepl("DEVELOPER WORKSTATION, PROFESSIONAL",toupper(sub$sku_desc)) &
                  grepl("WORKSTATION",toupper(sub$sku_desc)) == TRUE,
                  sub$sku_qty,0)
  one4 <- ifelse(grepl("OPENSHIFT",toupper(sub$sku_desc)) &
                 grepl("2 Core",toupper(sub$sku_desc)) &
                (grepl("ENTERPRISE",toupper(sub$sku_desc)) |
                 grepl("CONTAINER PLATFORM",toupper(sub$sku_desc))) == TRUE,
                 sub$sku_qty,0)
    two <- ifelse(grepl("PHYSICAL OR VIRTUAL NODES",toupper(sub$sku_desc)) |
                  grepl("DEVELOPER WORKSTATION, PROFESSIONAL",toupper(sub$sku_desc)) == TRUE,
                  sub$sku_qty,0)
  four1 <- ifelse(!grepl("LPAR",toupper(sub$sku_desc)) &
                   grepl("4 GUEST",toupper(sub$sku_desc)) == TRUE,
                   sub$sku_qty,0)
  four2 <- ifelse(grepl("DEVELOPER WORKSTATION, ENTERPRISE",toupper(sub$sku_desc)) == TRUE,
                  sub$sku_qty,0)
  four3 <- ifelse(grepl("4",toupper(sub$sku_desc)) &
                  grepl("LPAR",toupper(sub$sku_desc)) == TRUE,
                  sub$sku_qty,0)
  five <- ifelse(grepl("HYPERSCALE",toupper(sub$sku_desc)) == TRUE,
                 sub$sku_qty,0)
  ten <- ifelse(grepl("10 GUEST",toupper(sub$sku_desc)) == TRUE,
                sub$sku_qty,0)
  fifteen <- ifelse(grepl("15",toupper(sub$sku_desc)) &
                    grepl("LPAR",toupper(sub$sku_desc)) == TRUE,
                    sub$sku_qty,0)
  sixteen1 <- ifelse(grepl("UNLIMITED GUEST",toupper(sub$sku_desc)) |
                     grepl("VIRTUAL DATACENTER",toupper(sub$sku_desc)) |
                     grepl("DEVELOPER SUITE",toupper(sub$sku_desc)) |   
                     grepl("SMART VIRTUALIZATION",toupper(sub$sku_desc)) == TRUE,
                     sub$sku_qty,0)
  sixteen2 <- ifelse(!grepl("RED HAT CLOUD INFRASTRUCTURE (WITHOUT GUEST OS)",toupper(sub$sku_desc)) &
                      grepl("RED HAT CLOUD INFRASTRUCTURE",toupper(sub$sku_desc)) == TRUE,
                      sub$sku_qty,0)
  sixteen3 <- ifelse(!grepl("OPENSTACK PLATFORM (WITHOUT GUEST OS)",toupper(sub$sku_desc)) &
                      grepl("OPENSTACK PLATFORM",toupper(sub$sku_desc)) == TRUE,
                      sub$sku_qty,0)
  sixteen4 <- ifelse(!grepl("CLOUD SUITE (WITHOUT GUEST OS)",toupper(sub$sku_desc)) &
                      grepl("CLOUD SUITE",toupper(sub$sku_desc)) == TRUE,
                      sub$sku_qty,0)
  sixteen5 <- ifelse(grepl("OPENSHIFT",toupper(sub$sku_desc)) &
                     grepl("(1-2 SOCKETS)",toupper(sub$sku_desc)) &
                    (grepl("ENTERPRISE",toupper(sub$sku_desc)) |
                     grepl("CONTAINER PLATFORM",toupper(sub$sku_desc))) == TRUE,
                     sub$sku_qty,0)
  fifty <- ifelse(grepl("(50 PACK)",toupper(sub$sku_desc)) == TRUE,
                  sub$sku_qty,0)
  hundred <- ifelse(grepl("IBM SYSTEM Z",toupper(sub$sku_desc)) == TRUE,
                    sub$sku_qty,0)
  fourhundred <- ifelse(grepl("DEVELOPER SUPPORT",toupper(sub$sku_desc)) == TRUE,
                        sub$sku_qty,0)
  return (one1 + one2 + one3 + one4 +
           two * 3 +
           (four1 + four2 + four3) * 4 +
           five * 5 +
           ten * 10 +
           fifteen * 15 +
           (sixteen1 + sixteen2 + sixteen3 + sixteen4 + sixteen5)*16 )
}


#Connect to Red Shift Table
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-no-awssdk-1.2.10.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com:5439/rhdsrs?user=xyz&password=123"
conn <- dbConnect(driver, url)

#Read Salesforce download from local drive
dataset <- read.csv("C:/Users/beapen/Documents/Process Related/PROJECT/Closed_SEAP_Deals.csv")

#Clean the Dataset
finDataset <- CleanDataset(dataset)[c(1,14,4,7,15)]

#----------------Initialiation-----------------
finDataset$Checkin_OverDep <- numeric(nrow(finDataset))
finDataset$yum_mrepo_Spacewalk_cobbler <- character(nrow(finDataset))
finDataset$VDC_Subscriptions <- numeric(nrow(finDataset))
finDataset$Developer_Subscriptions <- numeric(nrow(finDataset))
finDataset$NFR_Subscriptions <- numeric(nrow(finDataset))
finDataset$RHEL_Eval_Subs_Months <- numeric(nrow(finDataset))

for(i in 1:nrow(finDataset)) #Looping through each account of the SFDC extract
{
  tmpAcc <- finDataset[i,2]
  dt <- mdy(finDataset[i,4])-30 #Assuming opportunity has been created one month later
  
  tmpSub <- subQuery(tmpAcc)
  tmpSub <- subset(tmpSub,tmpSub$service_start_date < dt & tmpSub$service_end_date >= dt)
  
  tmpChk <- chkQuery(tmpAcc)
  tmpChk$system_last_checkin_date <- ymd(tmpChk$system_last_checkin_date)
  tmpChk$profile_create_date <- as.Date(tmpChk$profile_create_date)
  
  #------------------------------- 3 Month Checkin OverDeployment ---------------------------------
  
  if(nrow(tmpChk)==0)
  {
    finDataset[i,6] <- 0
  }
  else
  {
    tmpChk <- CleanCheckin(tmpChk)
    
    dt90 <- as.Date(dt,"%y-%m-%d")-90
    tmpChk <- subset(tmpChk,between(as.Date(tmpChk$system_last_checkin_date,"%y-%m-%d"),dt90,dt))
    
    if(nrow(tmpChk)==0)
    {
      finDataset[i,6] <- 0
    }
    else
    {
      mul <- multiplier(tmpSub)
      tmpChk$Consider <- NA
      for(j in 1:nrow(tmpChk))
      {
        if(tmpChk[j,5]>=max(tmpChk$profile_create_date[tmpChk$hostname==tmpChk[j,2]]))
        {tmpChk[j,8]='Y'}
        else {tmpChk[j,8]='N'}
      }
      #if(length(tmpChk$Consider[tmpChk$Consider!="N"])>=10) {finDataset[i,6] <- "Y"} else {finDataset[i,6] <- "N"}
      ifelse(length(tmpChk$Consider[tmpChk$Consider!="N"]) - sum(mul) <= 0,
             finDataset[i,6] <- 0,
             finDataset[i,6] <- length(tmpChk$Consider[tmpChk$Consider!="N"]) - sum(mul))
      
    }
  }
  
  #------------------------------- Presence of YUM/MRepo/Spacewalk/Cobbler Servers ------------------
  
  if(nrow(tmpChk)==0)
  {
    finDataset[i,7] <- 0
  }
  else
  {
    tmpChk$yum_mrepo <- NA
    a <- grepl("yum",tmpChk$hostname,ignore.case = TRUE)
    b <- grepl("mrepo",tmpChk$hostname,ignore.case = TRUE)
    c <- grepl("spacewalk",tmpChk$hostname,ignore.case = TRUE)
    d <- grepl("cobbler",tmpChk$hostname,ignore.case = TRUE)
    if(any(c(a,b,c,d))) {tmpChk$yum_mrepo <- "Y"} else {tmpChk$yum_mrepo <- "N"}
    if(length(tmpChk$yum_mrepo[tmpChk$yum_mrepo!="N"]) > 0) {finDataset[i,7] <- 1} else {finDataset[i,7] <- 0}
  }
  
  #------------------------------- Number of VDC Subscriptions ------------------------------------
  
  VDC_Count <- 0
  if(nrow(tmpSub)>0)
  {
    VDC_Count <- ifelse(grepl("Virtual Datacenters",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  finDataset[i,8] <- sum(VDC_Count)
  
  #------------------------------- Number of Developer Subscriptions -------------------------------
  
  Developer_Count <- 0
  if(nrow(tmpSub)>0)
  {
    Developer_Count <- ifelse(grepl("Developer",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  finDataset[i,9] <- sum(Developer_Count)
  
  #------------------------------- Number of NFR Subscriptions -------------------------------------
  
  NFR_Count <- 0
  if(nrow(tmpSub)>0)
  {
    NFR_Count <- ifelse(grepl("NFR",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  finDataset[i,10] <- sum(NFR_Count)
  
  #------------------------------- Months of Eval Subscription Usage -------------------------------
  
  EVAL_Subs <- 0
  if(nrow(tmpSub)>0)
  {
    EVAL_Subs <- ifelse(grepl("Red Hat Enterprise Linux Server",tmpSub$sku_desc) & grepl("Evaluation",tmpSub$sku_desc)==TRUE,
                        round(time_length(interval(ymd(tmpSub$service_start_date),ymd(tmpSub$service_end_date)),"month"),0)*tmpSub$sku_qty,
                        0)
  }
  finDataset[i,11] <- sum(EVAL_Subs)
}

#-------------------------------- Reorder Dataset ------------------------------------------------

finDataset <- finDataset[,c(1,2,3,4,6,8,9,10,11,7,5)]

#-------------------------------- Convert to Numeric ---------------------------------------------

#finDataset[5:9] <- lapply(finDataset[5:9],as.numeric)

fd1 <- finDataset[-10]
fd1 <- fd1[-8]

fd1$Chk <- fd1$Checkin_OverDep + fd1$VDC_Subscriptions + fd1$Developer_Subscriptions + fd1$RHEL_Eval_Subs_Months

fd1 <- fd1[fd1$Chk!=0,]
fd1 <- fd1[-10]
