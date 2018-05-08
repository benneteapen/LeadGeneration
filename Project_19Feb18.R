library(RJDBC)
library(rJava)
library(stringr)
library(lubridate)
library(dplyr)
library(xgboost)

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

chkViews <- function(accno,dte)
{
  month(dte) <- month(dte) + 10
  fyq <- paste('FY',year(dte),'-Q',quarter(dte),sep='')
  views <- dbGetQuery(conn,sprintf("SELECT SUM(seap.portal.views) 
                                    FROM seap.portal 
                                    WHERE seap.portal.fyq = fyq
                                    AND seap.portal.customer_number = %s", accno))
  return(views[1,1])
}

chkCases <- function(accno,dte)
{
  month(dte) <- month(dte) + 10
  fyq <- paste('FY',year(dte),'-Q',quarter(dte),sep='')
  cases <- dbGetQuery(conn,sprintf("SELECT COUNT(*) 
                                    FROM seap.cases 
                                    WHERE seap.cases.fyq = fyq
                                    AND seap.cases.customer_number = %s", accno))
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

XGBoost <- function(xg)
{
  # Importing the dataset
  dataset <-  fd1
  dataset <-  dataset[4:13]
  
  # Encoding the categorical variables as factors
  dataset$Geo = as.numeric(factor(dataset$Geo,
                                  levels = c('EMEA', 'LATAM', 'APAC','NA'),
                                  labels = c(1, 2, 3, 4)))
  
  
  # Splitting the dataset into the Training set and Test set
  # install.packages('caTools')
  library(caTools)
  split = sample.split(dataset$DealSuccess, SplitRatio = 0.8)
  training_set = subset(dataset, split == TRUE)
  test_set = subset(dataset, split == FALSE)
  
  # Fitting XGBoost to the Training set
  # install.packages('xgboost')
  library(xgboost)
  classifier = xgboost(data = as.matrix(training_set[-10]), label = training_set$DealSuccess, nrounds = 115)
  
  # Predicting the Test set results
  y_pred = predict(classifier, newdata = as.matrix(test_set[-10]))
  y_pred = (y_pred >= 0.5)
  
  # Making the Confusion Matrix
  cm = table(test_set[, 10], y_pred)
  
  # Applying k-Fold Cross Validation
  # install.packages('caret')
  library(caret)
  folds = createFolds(training_set$DealSuccess, k = 10)
  cv <-  lapply(folds, function(x) {
    training_fold = training_set[-x, ]
    test_fold = training_set[x, ]
    classifier = xgboost(data = as.matrix(training_set[-10]), label = training_set$DealSuccess, nrounds = 115)
    y_pred = predict(classifier, newdata = as.matrix(test_fold[-10]))
    y_pred = (y_pred >= 0.5)
    cm = table(test_fold[, 10], y_pred)
    accuracy = (cm[1,1] + cm[2,2]) / (cm[1,1] + cm[2,2] + cm[1,2] + cm[2,1])
    return(accuracy)
  })
  accuracy = mean(as.numeric(cv))
}

#Connect to Red Shift Table
driver <- JDBC("com.amazon.redshift.jdbc41.Driver", "RedshiftJDBC41-no-awssdk-1.2.10.1009.jar", identifier.quote="`")
url <- "jdbc:redshift://rhdsrsprod.cpe1poooghvl.us-west-2.redshift.amazonaws.com:5439/rhdsrs?user=beapen&password=RHdsEUys7"
conn <- dbConnect(driver, url)

#Read Salesforce download from local drive ----------- TRAIN DATASET ----------------
dataset <- read.csv("C:/Users/beapen/Documents/Process Related/PROJECT/Closed_SEAP_Deals.csv")

#Clean the Dataset
TrainDataset <- CleanDataset(dataset)[c(1,14,4,7,15)]

#----------------Initialiation-----------------
TrainDataset$Checkin_OverDep             <- numeric(nrow(TrainDataset))
TrainDataset$yum_mrepo_Spacewalk_cobbler <- numeric(nrow(TrainDataset))
TrainDataset$VDC_Subscriptions           <- numeric(nrow(TrainDataset))
TrainDataset$Developer_Subscriptions     <- numeric(nrow(TrainDataset))
TrainDataset$NFR_Subscriptions           <- numeric(nrow(TrainDataset))
TrainDataset$RHEL_Eval_Subs_Months       <- numeric(nrow(TrainDataset))
TrainDataset$Views                       <- numeric(nrow(TrainDataset))
TrainDataset$Cases                       <- numeric(nrow(TrainDataset))


for(i in 1:1) #Looping through each account of the SFDC extract
{
  tmpAcc <- TrainDataset[i,2]
  dt <- mdy(TrainDataset[i,3])-30 #Assuming opportunity has been created one month later
  dt90 <- as.Date(dt,"%y-%m-%d")-90
  
  tmpSub <- subQuery(tmpAcc)
  tmpSub <- subset(tmpSub,tmpSub$service_start_date <= dt & tmpSub$service_end_date >= dt90)
  
  tmpChk <- chkQuery(tmpAcc)
  tmpChk$system_last_checkin_date <- ymd(tmpChk$system_last_checkin_date)
  tmpChk$profile_create_date <- as.Date(tmpChk$profile_create_date)
  
  #------------------------------- 3 Month Checkin OverDeployment ---------------------------------
  
  if(nrow(tmpChk)==0)
  {
    TrainDataset[i,6] <- 0
  }
  else
  {
    tmpChk <- CleanCheckin(tmpChk)
    
    dt90 <- as.Date(dt,"%y-%m-%d")-90
    tmpChk <- subset(tmpChk,between(as.Date(tmpChk$system_last_checkin_date,"%y-%m-%d"),dt90,dt))
    
    if(nrow(tmpChk)==0)
    {
      TrainDataset[i,6] <- 0
    }
    else
    {
      mul <- 0 #multiplier(tmpSub)
      tmpChk$Consider <- NA
      for(j in 1:nrow(tmpChk))
      {
        if(tmpChk[j,5]>=max(tmpChk$profile_create_date[tmpChk$hostname==tmpChk[j,2]]))
        {tmpChk[j,8]='Y'}
        else {tmpChk[j,8]='N'}
      }
      ifelse(length(tmpChk$Consider[tmpChk$Consider!="N"]) - sum(mul) <= 0,
             TrainDataset[i,6] <- 0,
             TrainDataset[i,6] <- length(tmpChk$Consider[tmpChk$Consider!="N"]) - sum(mul))
      
    }
  }
  
  #------------------------------- Presence of YUM/MRepo/Spacewalk/Cobbler Servers ------------------
  
  if(nrow(tmpChk)==0)
  {
    TrainDataset[i,7] <- 0
  }
  else
  {
    tmpChk$yum_mrepo <- NA
    a <- grepl("yum",tmpChk$hostname,ignore.case = TRUE)
    b <- grepl("mrepo",tmpChk$hostname,ignore.case = TRUE)
    c <- grepl("spacewalk",tmpChk$hostname,ignore.case = TRUE)
    d <- grepl("cobbler",tmpChk$hostname,ignore.case = TRUE)
    if(any(c(a,b,c,d))) {tmpChk$yum_mrepo <- "Y"} else {tmpChk$yum_mrepo <- "N"}
    if(length(tmpChk$yum_mrepo[tmpChk$yum_mrepo!="N"]) > 0) {TrainDataset[i,7] <- 1} else {TrainDataset[i,7] <- 0}
  }
  
  #------------------------------- Number of VDC Subscriptions ------------------------------------
  
  VDC_Count <- 0
  if(nrow(tmpSub)>0)
  {
    VDC_Count <- ifelse(grepl("Virtual Datacenters",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  TrainDataset[i,8] <- sum(VDC_Count)
  
  #------------------------------- Number of Developer Subscriptions -------------------------------
  
  Developer_Count <- 0
  if(nrow(tmpSub)>0)
  {
    Developer_Count <- ifelse(grepl("Developer",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  TrainDataset[i,9] <- sum(Developer_Count)
  
  #------------------------------- Number of NFR Subscriptions -------------------------------------
  
  NFR_Count <- 0
  if(nrow(tmpSub)>0)
  {
    NFR_Count <- ifelse(grepl("NFR",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  TrainDataset[i,10] <- sum(NFR_Count)
  
  #------------------------------- Months of Eval Subscription Usage -------------------------------
  
  EVAL_Subs <- 0
  if(nrow(tmpSub)>0)
  {
    EVAL_Subs <- ifelse(grepl("Evaluation",tmpSub$sku_desc)==TRUE,
                        round(time_length(interval(ymd(tmpSub$service_start_date),ymd(tmpSub$service_end_date)),"month"),0)*tmpSub$sku_qty,
                        0)
  }
  TrainDataset[i,11] <- sum(EVAL_Subs)
  
  
  #------------------------------- Number of Views -------------------------------------------------
  
  views <- chkViews(tmpAcc,dt)
  if(is.na(views)) TrainDataset[1,12] <- 0 else TrainDataset[i,12] <- views
  
  #------------------------------- Number of Cases--------------------------------------------------
  
  cases <- chkCases(tmpAcc,dt)
  TrainDataset[i,13] <- cases
  
  
}

#------------------------------- Reorder Dataset ---------------------------------------------------
TrainDataset <- TrainDataset[,c(1,2,4,3,6,8,9,10,11,12,13,7,5)]


#Read Accounts from Subscription Table -------- TEST DATASET --------------

TestDataset <- read.csv("C:/Users/beapen/Documents/Process Related/PROJECT/Accounts.csv")
levels <- levels(TestDataset$Geo)
levels[length(levels)+1] <- "NA"
TestDataset$Geo <- factor(TestDataset$Geo,levels = levels)
TestDataset$Geo[is.na(TestDataset$Geo)] <- "NA"

#----------------Initialiation-----------------
TestDataset$Checkin_OverDep             <- numeric(nrow(TestDataset))
TestDataset$VDC_Subscriptions           <- numeric(nrow(TestDataset))
TestDataset$Developer_Subscriptions     <- numeric(nrow(TestDataset))
TestDataset$NFR_Subscriptions           <- numeric(nrow(TestDataset))
TestDataset$RHEL_Eval_Subs_Months       <- numeric(nrow(TestDataset))
TestDataset$Views                       <- numeric(nrow(TestDataset))
TestDataset$Cases                       <- numeric(nrow(TestDataset))
TestDataset$yum_mrepo_Spacewalk_cobbler <- numeric(nrow(TestDataset))

for(i in 1743:nrow(TestDataset)) #Looping through each account of subscription data
{
  tmpAcc <- TestDataset[i,1]
  dt_st <- mdy('12/15/2017')
  dt_en <- mdy('03/14/2017')
  
  tmpSub <- subQuery(tmpAcc)
  tmpSub <- subset(tmpSub,tmpSub$service_start_date <= dt_en & tmpSub$service_end_date >= dt_st)
  
  tmpChk <- chkQuery(tmpAcc)
  tmpChk$system_last_checkin_date <- ymd(tmpChk$system_last_checkin_date)
  tmpChk$profile_create_date <- as.Date(tmpChk$profile_create_date)
  
  #------------------------------- 3 Month Checkin OverDeployment ---------------------------------
  
  if(nrow(tmpChk)==0)
  {
    TestDataset[i,3] <- 0
  }
  else
  {
    tmpChk <- CleanCheckin(tmpChk)
    
    tmpChk <- subset(tmpChk,between(as.Date(tmpChk$system_last_checkin_date,"%y-%m-%d"),dt_st,dt_en))
    
    if(nrow(tmpChk)==0)
    {
      TestDataset[i,3] <- 0
    }
    else
    {
      mul <- 0 #multiplier(tmpSub)
      tmpChk$Consider <- NA
      for(j in 1:nrow(tmpChk))
      {
        if(tmpChk[j,5]>=max(tmpChk$profile_create_date[tmpChk$hostname==tmpChk[j,2]]))
        {tmpChk[j,8]='Y'}
        else {tmpChk[j,8]='N'}
      }
      ifelse(length(tmpChk$Consider[tmpChk$Consider!="N"]) - sum(mul) <= 0,
             TestDataset[i,3] <- 0,
             TestDataset[i,3] <- length(tmpChk$Consider[tmpChk$Consider!="N"]) - sum(mul))
      
    }
  }
  
  #------------------------------- Number of VDC Subscriptions ------------------------------------
  
  VDC_Count <- 0
  if(nrow(tmpSub)>0)
  {
    VDC_Count <- ifelse(grepl("Virtual Datacenters",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  TestDataset[i,4] <- sum(VDC_Count)
  
  #------------------------------- Number of Developer Subscriptions -------------------------------
  
  Developer_Count <- 0
  if(nrow(tmpSub)>0)
  {
    Developer_Count <- ifelse(grepl("Developer",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  TestDataset[i,5] <- sum(Developer_Count)
  
  #------------------------------- Number of NFR Subscriptions -------------------------------------
  
  NFR_Count <- 0
  if(nrow(tmpSub)>0)
  {
    NFR_Count <- ifelse(grepl("NFR",tmpSub$sku_desc), tmpSub$sku_qty, 0)
  }
  TestDataset[i,6] <- sum(NFR_Count)
  
  #------------------------------- Months of Eval Subscription Usage -------------------------------
  
  EVAL_Subs <- 0
  if(nrow(tmpSub)>0)
  {
    EVAL_Subs <- ifelse(grepl("Evaluation",tmpSub$sku_desc)==TRUE,
                        round(time_length(interval(ymd(tmpSub$service_start_date),ymd(tmpSub$service_end_date)),"month"),0)*tmpSub$sku_qty,
                        0)
  }
  TestDataset[i,7] <- sum(EVAL_Subs)
  
  #------------------------------- Number of Views -------------------------------------------------
  
  views <- chkViews(tmpAcc,dt_st)
  if(is.na(views)) TestDataset[1,8] <- 0 else TestDataset[i,8] <- views
  
  #------------------------------- Number of Cases--------------------------------------------------
  
  cases <- chkCases(tmpAcc,dt_st)
  TestDataset[i,9] <- cases
  
  #------------------------------- Presence of YUM/MRepo/Spacewalk/Cobbler Servers ------------------
  
  if(nrow(tmpChk)==0)
  {
    TestDataset[i,4] <- 0
  }
  else
  {
    tmpChk$yum_mrepo <- NA
    a <- grepl("yum",tmpChk$hostname,ignore.case = TRUE)
    b <- grepl("mrepo",tmpChk$hostname,ignore.case = TRUE)
    c <- grepl("spacewalk",tmpChk$hostname,ignore.case = TRUE)
    d <- grepl("cobbler",tmpChk$hostname,ignore.case = TRUE)
    if(any(c(a,b,c,d))) {tmpChk$yum_mrepo <- "Y"} else {tmpChk$yum_mrepo <- "N"}
    if(length(tmpChk$yum_mrepo[tmpChk$yum_mrepo!="N"]) > 0) {TestDataset[i,10] <- 1} else {TestDataset[i,10] <- 0}
  }
  
 
}
