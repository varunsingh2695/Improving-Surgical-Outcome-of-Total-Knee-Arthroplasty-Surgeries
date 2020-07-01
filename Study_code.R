#install.packages("reshape")
library(reshape)
library(sqldf)
#install.packages("pastecs")
library(pastecs)
# run after running till line 1176
# install.packages("bit64")
#install.packages("parallel")
#require(parallel)
#install.packages("geosphere")
library(geosphere)
library(MASS)
#install.packages("zipcode")
#library(zipcode)
library(dplyr)



#---------------------------------------------------------------------------------------------------------------------------------

options(java.parameters = "-Xmx64048m") # 64048 is 64 GB

#install.packages("odbc")

#nstall.packages("RMariaDB")

library(RMariaDB)

# Connect to a MariaDB version of a MySQL database

con <- dbConnect(RMariaDB::MariaDB(), host="datamine.rcac.purdue.edu", port=3306
                 
                 , dbname="bonutti"
                 
                 , user="bonutti_user", password="novelhealthcare")

# list of db tables

dbListTables(con)

#---------------------------------------------------------------------------------------------------------------------------------



#--------------------MedLists start



MedLists <- dbGetQuery(con, "SELECT * FROM MedLists")



#--------------------MedLists end





#--------------------PatientVisitProcs start



PatientVisitProcs <- dbGetQuery(con, "SELECT * FROM PatientVisitProcs")

keep <- c("PatientVisitProcsId","BatchId","DateOfEntry","OrderCodeId","OrderId","ProceduresId","PatientVisitId","PatientVisitDiags1","PatientVisitDiags2","PatientVisitDiags3","PatientVisitDiags4","PatientVisitDiags5","PatientVisitDiags6","PatientVisitDiags7","PatientVisitDiags8","PatientVisitDiags9","Code","CPTCode","Description","PlaceOfServiceMId","DateOfServiceFrom","DateOfServiceTo","Modifier1MId","Modifier2MId","Modifier3MId","Modifier4MId")

patient_visit_procs <- PatientVisitProcs[keep]

patient_visit_procs <- subset(patient_visit_procs, CPTCode == "27446" | CPTCode == "27447" | CPTCode == "27486" | CPTCode == "27487" | CPTCode == "27488" | CPTCode == "99024" | CPTCode == "99211" | CPTCode == "99212" | CPTCode == "99213" | CPTCode == "99214" | CPTCode == "99215",)



med_lists_patient_visit_procs <- subset(MedLists,TableName=="PlaceOfServiceCodes",c("MedListsId","Code","Description"))

patient_visit_procs <- merge(patient_visit_procs, med_lists_patient_visit_procs, by.x="PlaceOfServiceMId", by.y="MedListsId", all.x = T)

patient_visit_procs <- reshape::rename(patient_visit_procs,c(Code.x="Code",Code.y="code_place_of_service_mid",Description.x="Description",Description.y="description_place_of_service_mid"))

patient_visit_procs <- as.data.frame(patient_visit_procs)





med_lists_patient_visit_procs <- subset(MedLists,TableName=="Modifiers",c("MedListsId","Code","Description"))

patient_visit_procs <- merge(patient_visit_procs, med_lists_patient_visit_procs, by.x="Modifier1MId", by.y="MedListsId", all.x = T)

patient_visit_procs <- reshape::rename(patient_visit_procs,c(Code.x="Code",Code.y="code_modifier_1_mid",Description.x="Description",Description.y="description_modifier_1_mid"))

patient_visit_procs <- as.data.frame(patient_visit_procs)





med_lists_patient_visit_procs <- subset(MedLists,TableName=="Modifiers",c("MedListsId","Code","Description"))

patient_visit_procs <- merge(patient_visit_procs, med_lists_patient_visit_procs, by.x="Modifier2MId", by.y="MedListsId", all.x = T)

patient_visit_procs <- reshape::rename(patient_visit_procs,c(Code.x="Code",Code.y="code_modifier_2_mid",Description.x="Description",Description.y="description_modifier_2_mid"))

patient_visit_procs <- as.data.frame(patient_visit_procs)



patient_visit_procs$modifier <- ifelse(patient_visit_procs$description_modifier_1_mid == c("Left", "Right", "Bilateral Procedure"), patient_visit_procs$description_modifier_1_mid, NA)

patient_visit_procs$modifier <- ifelse(is.na(patient_visit_procs$modifier), patient_visit_procs$description_modifier_2_mid, patient_visit_procs$modifier)

patient_visit_procs$modifier <- ifelse(patient_visit_procs$modifier == c("Left", "Right", "Bilateral Procedure"), patient_visit_procs$modifier, NA)



patient_visit_procs$modifier <- ifelse((patient_visit_procs$CPTCode == "27446" | patient_visit_procs$CPTCode == "27447" | patient_visit_procs$CPTCode == "27486" | patient_visit_procs$CPTCode == "27487" | patient_visit_procs$CPTCode == "27488") & is.na(patient_visit_procs$modifier),
                                       
                                       "surgery", patient_visit_procs$modifier)



#--------------------PatientVisitProcs end





#--------------------PatientVisit start



PatientVisit <- dbGetQuery(con, "SELECT * FROM PatientVisit")

keep <- c("PatientVisitId","Entered","PatientProfileId","FinancialClassMId","DoctorId","FacilityId","CompanyId","Visit","BillStatus","ClaimStatus","CurrentCarrier","FirstFiledDate","LastFiledDate","TicketNumber")

patient_visit <- PatientVisit[keep]



## Medlists Financial class status

med_lists_patient_visit <- subset(MedLists,TableName=="FinancialClass",c("MedListsId","Code","Description"))

patient_visit$FinancialClassMId <- as.numeric(patient_visit$FinancialClassMId)

med_lists_patient_visit$MedListsId <- as.numeric(med_lists_patient_visit$MedListsId)

patient_visit <- merge(patient_visit, med_lists_patient_visit, by.x="FinancialClassMId", by.y="MedListsId", all.x = T)

patient_visit <- reshape::rename(patient_visit,c(Code="code_financial_class_mid",Description="description_financial_class_mid"))



#--------------------PatientVisit end





#--------------------PatientProfile start



PatientProfile <- dbGetQuery(con, "SELECT * FROM PatientProfile")

keep <- c("PatientProfileId","PatientId","PId","PatientStatusMId","City","State","Zip","Country","AddressType","Birthdate","DeathDate","Sex","MaritalStatusMId","FacilityId","PrimaryCareDoctorId","ProfileNotes","AlertNotes","BillingNotes","NewPatient","EmpStatusMId")

patient_profile <- PatientProfile[keep]



## Medlists Patient Profile Status

med_lists_patient_profile <- subset(MedLists,TableName=="PatientProfileStatus",c("MedListsId","Code","Description"))

patient_profile$PatientStatusMId <- as.numeric(patient_profile$PatientStatusMId)

med_lists_patient_profile$MedListsId <- as.numeric(med_lists_patient_profile$MedListsId)

patient_profile <- merge(patient_profile, med_lists_patient_profile, by.x="PatientStatusMId", by.y="MedListsId", all.x = T)

patient_profile <- reshape::rename(patient_profile,c(Code="code_patient_status_mid",Description="description_patient_status_mid"))



## Medlists EmpStatus

med_lists_patient_profile <- subset(MedLists,TableName=="EmploymentStatus",c("MedListsId","Code","Description"))

patient_profile$EmpStatusMId <- as.numeric(patient_profile$EmpStatusMId)

med_lists_patient_profile$MedListsId <- as.numeric(med_lists_patient_profile$MedListsId)

patient_profile <- merge(patient_profile, med_lists_patient_profile, by.x="EmpStatusMId", by.y="MedListsId", all.x = T)

patient_profile <- reshape::rename(patient_profile,c(Code="code_emp_status_mid",Description="description_emp_status_mid"))



## Medlists Marital Status

med_lists_patient_profile <- subset(MedLists,TableName=="MaritalStatus",c("MedListsId","Code","Description"))

patient_profile$MaritalStatusMId <- as.numeric(patient_profile$MaritalStatusMId)

med_lists_patient_profile$MedListsId <- as.numeric(med_lists_patient_profile$MedListsId)

patient_profile <- merge(patient_profile, med_lists_patient_profile, by.x="MaritalStatusMId", by.y="MedListsId", all.x = T)

patient_profile <- reshape::rename(patient_profile,c(Code="code_marital_status_mid",Description="description_marital_status_mid"))



#--------------------PatientProfile end



#--------------------PatientRace start



PatientRace <- dbGetQuery(con, "SELECT * FROM PatientRace")

keep <- c("PatientRaceId","PID","PatientProfileId","PatientRaceMid")

patient_race <- PatientRace[keep]

patient_race <- patient_race[!duplicated(patient_race$PatientProfileId),]



## Medlists Race

med_lists_patient_race <- subset(MedLists,TableName=="Race",c("MedListsId","Code","Description"))

patient_race$PatientRaceMid <- as.numeric(patient_race$PatientRaceMid)

med_lists_patient_race$MedListsId <- as.numeric(med_lists_patient_race$MedListsId)

patient_race <- merge(patient_race, med_lists_patient_race, by.x="PatientRaceMid", by.y="MedListsId", all.x = T)

patient_race <- reshape::rename(patient_race,c(Code="code_patient_race_mid",Description="description_code_patient_race_mid"))



#--------------------PatientRace end





#--------------------PatientVisitDiags start



PatientVisitDiags <- dbGetQuery(con, "SELECT * FROM PatientVisitDiags")

keep <- c("PatientVisitDiagsId","DiagnosisId","OrderDiagId","PatientVisitId","Code","Description","ICD9Code")

patient_visit_diags <- PatientVisitDiags[keep]



#--------------------PatientVisitDiags end



#--------------------Procedures start



Procedures<- dbGetQuery(con, "SELECT * FROM Procedures")

keep <- c("ProceduresId","MasterProceduresId","Inactive","Code","CPTCode","RevenueCode","Description","DescriptionLong","TypeOfServiceMId")

procedures <- Procedures[keep]



## Medlists Type of service

med_lists_procedures <- subset(MedLists,TableName=="TypeOfServiceCodes",c("MedListsId","Code","Description"))

procedures$TypeOfServiceMId <- as.numeric(procedures$TypeOfServiceMId)

med_lists_procedures$MedListsId <- as.numeric(med_lists_procedures$MedListsId)

procedures <- merge(procedures, med_lists_procedures, by.x="TypeOfServiceMId", by.y="MedListsId", all.x = T)

procedures <- reshape::rename(procedures,c(Code.x="Code",Code.y="code_type_of_service_mid",Description.x="Description",Description.y="description_type_of_service_mid"))



#--------------------Procedures end



#--------------------PROBLEM start



PROBLEM <- dbGetQuery(con, "SELECT * FROM PROBLEM")

keep <- c("PID","PRID","SPRID","XID","SDID","USRID","QUALIFIER","CODE","DESCRIPTION","ONSETDATE","STOPDATE","STOPREASON","PRIORITY","ICD9MasterDiagnosisId","ICD10MasterDiagnosisId","SNOMEDMasterDiagnosisId")

problem <- PROBLEM[keep]



#--------------------PROBLEM end



#--------------------ALLERGY start



ALLERGY <- dbGetQuery(con, "SELECT * FROM ALLERGY")

keep <- c("PID","XID","AID","CHANGE","SDID","USRID","NAME","RASH","SHOCK","RESP","GI","HEME","OTHER","DESCRIPTION","ONSETDATE","STOPDATE","STOPREASON")

allergy <- ALLERGY[keep]



#--------------------ALLERGY end



#--------------------OBS end



OBS <- dbGetQuery(con, "SELECT * FROM OBS")

keep <- c("PID", "XID", "OBSID", "CHANGE", "SDID", "USRID", "HDID", "OBSVALUE")

obs <- OBS[keep]



obs_height <- subset(obs,HDID == "55",c("PID", "OBSVALUE"))

obs_height <- reshape::rename(obs_height,c(OBSVALUE="height"))

obs_height$height <- as.numeric(obs_height$height)



obs_weight <- subset(obs,HDID == "61",c("PID", "OBSVALUE"))

obs_weight <- reshape::rename(obs_weight,c(OBSVALUE="weight"))

obs_weight$weight <- as.numeric(obs_weight$weight)

obs_weight <- subset(obs_weight, !is.na(weight),)



## OBS height weight

obs_height <- obs_height[!duplicated(obs_height$PID),]

obs_weight <- obs_weight[!duplicated(obs_weight$PID),]

obs_height_weight <- merge(obs_height,obs_weight,by="PID", all = T)



#--------------------OBS end



rm(con)

#----------------------------------------------------------------------------------------------------------------------------------------



##-------------------merging datasets start

patient_visit_procs_1 <- patient_visit_procs

patient_visit_procs_1$CPTCode <- as.numeric(patient_visit_procs_1$CPTCode)

patient_visit_procs_1 <-patient_visit_procs[order(-patient_visit_procs_1$CPTCode),]

patient_visit_procs_1 <- patient_visit_procs_1[!duplicated(patient_visit_procs_1$PatientVisitId),]



#patient visit procs & patient visit

final <- merge(patient_visit_procs_1, patient_visit, by="PatientVisitId", all.x = T)

table(final$description_financial_class_mid, useNA = "ifany")



#patient profile

final_1 <- merge(final, patient_profile, by="PatientProfileId", all.x = T)

table(final_1$FacilityId.x, final_1$FacilityId.y, useNA = "ifany")



#patient race

final_2 <- merge(final_1, patient_race, by="PatientProfileId", all.x = T)



#patient visit diags
#table(patient_visit_diags_1$ICD9Code, useNA = "ifany")
patient_visit_diags_1 <- subset(patient_visit_diags,ICD9Code=="996.4" | (ICD9Code>="996.40" & ICD9Code<="996.47") | ICD9Code=="996.49" | 
                                  ICD9Code=="T84.012A" | ICD9Code=="T84.012D" | ICD9Code=="T84.012S" | ICD9Code=="T84.012" |
                                  ICD9Code=="T84.013A" | ICD9Code=="T84.013D" | ICD9Code=="T84.013S" | ICD9Code=="T84.013" | 
                                  ICD9Code=="T84.022A" | ICD9Code=="T84.022D" | ICD9Code=="T84.022S" | ICD9Code=="T84.022" |
                                  ICD9Code=="T84.023A" | ICD9Code=="T84.023D" | ICD9Code=="T84.023S" | ICD9Code=="T84.023" | 
                                  ICD9Code=="T84.032A" | ICD9Code=="T84.032D" | ICD9Code=="T84.032S" | ICD9Code=="T84.032" |
                                  ICD9Code=="T84.033A" | ICD9Code=="T84.033D" | ICD9Code=="T84.033S" | ICD9Code=="T84.033" | 
                                  ICD9Code=="T84.052A" | ICD9Code=="T84.052D" | ICD9Code=="T84.052S" | ICD9Code=="T84.052" | 
                                  ICD9Code=="T84.053A" | ICD9Code=="T84.053D" | ICD9Code=="T84.053S" | ICD9Code=="T84.053" | 
                                  ICD9Code=="T84.062A" | ICD9Code=="T84.062D" | ICD9Code=="T84.062S" | ICD9Code=="T84.062" | 
                                  ICD9Code=="T84.092A" | ICD9Code=="T84.092D" | ICD9Code=="T84.092S" | ICD9Code=="T84.092" | 
                                  ICD9Code=="T84.093A" | ICD9Code=="T84.093D" | ICD9Code=="T84.093S" | ICD9Code=="T84.093" | 
                                  ICD9Code=="T81.31XA" | ICD9Code=="T81.31XD" | ICD9Code=="T81.31XS" | ICD9Code=="T81.31" | 
                                  ICD9Code=="T81.32XA" | ICD9Code=="T81.32XD" | ICD9Code=="T81.32XS" | ICD9Code=="T81.32" | 
                                  ICD9Code=="T81.41XA" | ICD9Code=="T81.41XD" | ICD9Code=="T81.41XS" | ICD9Code=="T81.41" | 
                                  ICD9Code=="T81.42XA" | ICD9Code=="T81.42XD" | ICD9Code=="T81.42XS" | ICD9Code=="T81.42" | 
                                  ICD9Code=="T81.49XA" | ICD9Code=="T81.49XD" | ICD9Code=="T81.49XS" | ICD9Code=="T81.49" | 
                                  ICD9Code=="T81.89XA" | ICD9Code=="T81.89XD" | ICD9Code=="T81.89XS" | ICD9Code=="T81.89" | 
                                  ICD9Code=="T81.9XXA" | ICD9Code=="T81.9XXD" | ICD9Code=="T81.9XXS" | ICD9Code=="T81.9" | 
                                  ICD9Code=="M97.1" | ICD9Code=="M97.1" | ICD9Code=="M97.1" | ICD9Code=="M97.1" | 
                                  ICD9Code=="M97.11XA" | ICD9Code=="M97.11XD" | ICD9Code=="M97.11XS" | ICD9Code=="M97.11" | 
                                  ICD9Code=="M97.12XA" | ICD9Code=="M97.12XD" | ICD9Code=="M97.12XS" | ICD9Code=="M97.12" | 
                                  ICD9Code=="T84.042A" | ICD9Code=="T84.042D" | ICD9Code=="T84.042S" | ICD9Code=="T84.042" | 
                                  ICD9Code=="T84.043A" | ICD9Code=="T84.043D" | ICD9Code=="T84.043S" | ICD9Code=="T84.043" | 
                                  ICD9Code=="998.11" | ICD9Code=="998.12" | ICD9Code=="998.13" | ICD9Code=="998.30" | 
                                  ICD9Code=="998.31" | ICD9Code=="998.32" | ICD9Code=="998.59" | ICD9Code=="998.83" | 
                                  ICD9Code=="998.89" | ICD9Code=="998.9" | ICD9Code=="996.66" | ICD9Code=="996.69" | 
                                  ICD9Code=="996.77" | ICD9Code=="996.78" | ICD9Code=="996.79",)

patient_visit_diags_1 <- patient_visit_diags_1[!duplicated(patient_visit_diags_1$PatientVisitId),]

patient_visit_diags_1$PatientVisitId <- as.numeric(patient_visit_diags_1$PatientVisitId)

final_2$PatientVisitId <- as.numeric(final_2$PatientVisitId)

final_3 <- merge(final_2, patient_visit_diags_1, by="PatientVisitId", all.x = T)



final_3$Code.y <- NULL

final_3 <- reshape::rename(final_3,c(Code.x="Code"))

final_3$Description.y <- NULL

final_3 <- reshape::rename(final_3,c(Description.x="Description"))



#procedures



final_4 <- merge(final_3, procedures, by="ProceduresId", all.x = T)



final_4$Code.y <- NULL

final_4 <- reshape::rename(final_4,c(Code.x="Code"))

final_4$CPTCode.y <- NULL

final_4 <- reshape::rename(final_4,c(CPTCode.x="CPTCode"))

final_4$Description.y <- NULL

final_4 <- reshape::rename(final_4,c(Description.x="Description"))



#problem

final_5 <- merge(final_4, problem, by.x="PatientId", by.y="PID", all.x = T)



#allergy

allergy_1 <- allergy

allergy_1 <- subset(allergy_1,is.na(STOPREASON),)

allergy_1 <- allergy_1[!duplicated(allergy_1$PID),]

allergy_1$allergy <- "Yes"

allergy_1 <- subset(allergy_1,,c("PID","allergy"))



final_6 <- merge(final_5, allergy_1, by.x="PId", by.y="PID", all.x = T)

final_6$allergy <- ifelse(is.na(final_6$allergy), "No", final_6$allergy)

table(final_6$allergy, useNA = "ifany")



#obs_height_weight

names(obs_height_weight)

final_7 <- merge(final_6,obs_height_weight,by.x="PId",by.y="PID",all.x=T)



##-------------------merging datasets end



##-------------------subsetting sergeries and visits data



visits <- subset(final_7, CPTCode != "27446" & CPTCode != "27447" & CPTCode != "27486" & CPTCode != "27487" & CPTCode != "27488",)

surgeries <- subset(final_7, CPTCode == "27446" | CPTCode == "27447" | CPTCode == "27486" | CPTCode == "27487" | CPTCode == "27488",)



surgeries_1 <- surgeries

surgeries_1$code_modifier_1_mid <- ifelse(surgeries_1$code_modifier_1_mid=="AS","shikhar",surgeries_1$code_modifier_1_mid)

surgeries_1$code_modifier_1_mid <- ifelse(is.na(surgeries_1$code_modifier_1_mid),"anish",surgeries_1$code_modifier_1_mid)

table(surgeries_1$code_modifier_1_mid, useNA = "ifany")



surgeries_1 <- subset(surgeries_1,code_modifier_1_mid!="shikhar",)

surgeries_1 <- subset(surgeries_1,code_modifier_1_mid!="80",)

table(surgeries_1$code_modifier_1_mid, useNA = "ifany")





surgeries_2 <- surgeries_1

table(surgeries_2$code_modifier_2_mid, useNA = "ifany")

surgeries_2$code_modifier_2_mid <- ifelse(surgeries_2$code_modifier_2_mid=="AS","shikhar",surgeries_2$code_modifier_2_mid)

surgeries_2$code_modifier_2_mid <- ifelse(is.na(surgeries_2$code_modifier_2_mid),"anish",surgeries_2$code_modifier_2_mid)

surgeries_2 <- subset(surgeries_2,code_modifier_2_mid!="shikhar",)

surgeries_2 <- surgeries_2[surgeries_2$code_modifier_2_mid !="AS",]





surgeries_3 <- surgeries_2



surgeries_3$remove <- ifelse((surgeries_3$PatientProfileId=="1294" & surgeries_3$CPTCode=="27488") | 
                               
                               (surgeries_3$PatientProfileId=="1826" & surgeries_3$CPTCode=="27488") |
                               
                               (surgeries_3$PatientProfileId=="8462" & surgeries_3$CPTCode=="27486") |
                               
                               (surgeries_3$PatientProfileId=="13834" & surgeries_3$code_modifier_1_mid=="81") |
                               
                               (surgeries_3$PatientProfileId=="19769" & surgeries_3$PatientVisitId=="198937") |
                               
                               (surgeries_3$PatientProfileId=="80399" & surgeries_3$code_modifier_1_mid=="81") |
                               
                               (surgeries_3$PatientProfileId=="87775" & surgeries_3$code_modifier_1_mid=="81") |
                               
                               (surgeries_3$PatientProfileId=="95172" & surgeries_3$code_modifier_1_mid=="81"),1,0)





surgeries_3 <- subset(surgeries_3,remove != 1,)



surgeries_4 <- surgeries_3

surgeries_4$modifier <- ifelse(surgeries_4$code_modifier_1_mid=="LT" | surgeries_4$code_modifier_2_mid=="LT","Left",surgeries_4$modifier)

surgeries_4$modifier <- ifelse(surgeries_4$code_modifier_1_mid=="RT" | surgeries_4$code_modifier_2_mid=="RT","Right",surgeries_4$modifier)

surgeries_4$modifier <- ifelse(surgeries_4$code_modifier_1_mid=="50" | surgeries_4$code_modifier_2_mid=="50","Bilateral Procedure",surgeries_4$modifier)

table(surgeries_4$modifier, useNA = "ifany")





##----------------------------post-op and complication calculations



# surgeries_modifiers <- subset(surgeries_4, modifier!="surgery",)

# surgeries_no_modifiers <- subset(surgeries_4, modifier=="surgery",)



surgeries_5 <- surgeries_4


### For unique PatientProfileId 

surgeries_5 <- surgeries_5 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())

surgeries_5 <- surgeries_5[order(surgeries_5$PatientProfileId, -surgeries_5$Count),]

surgeries_5 <- surgeries_5[!duplicated(surgeries_5$PatientProfileId),]

surgeries_5 <- subset(surgeries_5,Count==1,)

table(surgeries_5$CPTCode, useNA = "ifany")



surgeries_unique_revision <- subset(surgeries_5,CPTCode=="27486" | CPTCode=="27487" | CPTCode=="27488",)



surgeries_6 <- subset(surgeries_5,CPTCode=="27446" | CPTCode=="27447",)

surgeries_6$surgery_date <- as.Date(as.POSIXct(surgeries_6$Visit, origin="1970-01-01"))

surgeries_6$one_year_from_surgery <- surgeries_6$surgery_date + 364





visits$visit_date <- as.Date(as.POSIXct(visits$Visit, origin="1970-01-01"))



##------------------actual data ends here





##------------------post-op calc for unique PatientProfileId

surgeries_7 <- surgeries_6[,c("PatientProfileId","surgery_date","one_year_from_surgery")]

visits_1 <- visits[,c("PatientProfileId","visit_date")]



patient <- surgeries_7$PatientProfileId

visits_1 <- visits_1[visits_1$PatientProfileId %in% patient,]


visits_1$concat <- paste0(visits_1$PatientProfileId,visits_1$visit_date)
visits_1 <- visits_1[!duplicated(visits_1$concat),]
visits_1$concat <- NULL


length(unique(visits_1$concat))



surgeries_visits <- merge(visits_1,surgeries_7,by="PatientProfileId",all.x = T)
surgeries_visits$concat <- paste0(surgeries_visits$PatientProfileId,surgeries_visits$visit_date)
surgeries_visits$visit_yes_no <- ifelse(surgeries_visits$visit_date > surgeries_visits$surgery_date &
                                          surgeries_visits$visit_date <= surgeries_visits$one_year_from_surgery,1,0)

table(surgeries_visits$visit_yes_no, useNA = "ifany")

surgeries_visits <- subset(surgeries_visits,visit_yes_no==1,)

surgeries_visits_1 <- surgeries_visits %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
surgeries_visits_1 <- surgeries_visits_1[order(surgeries_visits_1$PatientProfileId,-surgeries_visits_1$Count),]
surgeries_visits_1 <- surgeries_visits_1[!duplicated(surgeries_visits_1$PatientProfileId),]

surgeries_visits_2 <- subset(surgeries_visits_1,,c("PatientProfileId","Count"))
surgeries_visits_3 <- merge(surgeries_7[c("PatientProfileId")],surgeries_visits_2,by="PatientProfileId",all.x = T)
table(surgeries_visits_3$Count, useNA = "ifany")
surgeries_visits_3$Count <- ifelse(is.na(surgeries_visits_3$Count),0,surgeries_visits_3$Count)

surgeries_visits_3$PatientProfileId <- as.numeric(surgeries_visits_3$PatientProfileId)

nrow(surgeries_7)
length(unique(surgeries_visits$concat))










### For non unique PatientProfileId


surgeries_8 <- surgeries_4


### For unique PatientProfileId 

surgeries_8 <- surgeries_8 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())

surgeries_8 <- surgeries_8[order(surgeries_8$PatientProfileId, -surgeries_8$Count),]

surgeries_8 <- surgeries_8[!duplicated(surgeries_8$PatientProfileId),]

surgeries_8 <- subset(surgeries_8,Count!=1,)

surgeries_8$present <- 1

head(surgeries_8$present,20)

surgeries_9 <- merge(surgeries_4,surgeries_8[c("PatientProfileId","Count")],by="PatientProfileId",all.x = T)

#surgeries_10 <- subset(surgeries_9,!is.na(Count),)
surgeries_10 <- surgeries_9
surgeries_11 <- subset(surgeries_10,,c("PatientProfileId","Visit","modifier", "CPTCode"))

surgeries_11 <- surgeries_11 %>% arrange(PatientProfileId,as.Date(Visit))

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())

table(surgeries_11$CPTCode,surgeries_11$Count , useNA = "ifany")

### removing 1st time revision surgeries

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)

surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)

surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)



surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)


surgeries_11 <- surgeries_11[,-5]

surgeries_11 <- surgeries_11 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
table(surgeries_11$CPTCode, surgeries_11$Count, useNA = "ifany")

surgeries_11 <- subset(surgeries_11,(CPTCode == "27446" | CPTCode == "27447") | ((CPTCode == "27486" |CPTCode == "27487"| CPTCode == "27488" ) &
                                                                                   Count != 1),)

table(surgeries_11$CPTCode, useNA = "ifany")



surgeries_12 <- surgeries_11
surgeries_12 <- surgeries_12[,-5]

surgeries_12 <- surgeries_12 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
surgeries_12 <- surgeries_12[order(surgeries_12$PatientProfileId, -surgeries_12$Count),]
surgeries_13 <- surgeries_12[!duplicated(surgeries_12$PatientProfileId),]

surgeries_13 <- subset(surgeries_13,Count==1,)


###

surgeries_13 <- surgeries_13[,c("PatientProfileId","Visit")]
surgeries_13$surgery_date <- as.Date(as.POSIXct(surgeries_13$Visit, origin="1970-01-01"))
surgeries_13$Visit <- NULL
surgeries_13$one_year_from_surgery <- surgeries_13$surgery_date + 364


visits_1 <- visits[,c("PatientProfileId","visit_date")]
patient <- surgeries_13$PatientProfileId
visits_1 <- visits_1[visits_1$PatientProfileId %in% patient,]


visits_1$concat <- paste0(visits_1$PatientProfileId,visits_1$visit_date)
visits_1 <- visits_1[!duplicated(visits_1$concat),]
visits_1$concat <- NULL


surgeries_visits_b <- merge(visits_1,surgeries_13,by="PatientProfileId",all.x = T)
surgeries_visits_b$visit_yes_no <- ifelse(surgeries_visits_b$visit_date > surgeries_visits_b$surgery_date &
                                            surgeries_visits_b$visit_date <= surgeries_visits_b$one_year_from_surgery,1,0)

table(surgeries_visits_b$visit_yes_no, useNA = "ifany")

surgeries_visits_b <- subset(surgeries_visits_b,visit_yes_no==1,)

surgeries_visits_b_1 <- surgeries_visits_b %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
surgeries_visits_b_1 <- surgeries_visits_b_1[order(surgeries_visits_b_1$PatientProfileId,-surgeries_visits_b_1$Count),]
surgeries_visits_b_1 <- surgeries_visits_b_1[!duplicated(surgeries_visits_b_1$PatientProfileId),]

surgeries_visits_b_2 <- subset(surgeries_visits_b_1,,c("PatientProfileId","Count"))
surgeries_visits_b_3 <- merge(surgeries_13[c("PatientProfileId")],surgeries_visits_b_2,by="PatientProfileId",all.x = T)
table(surgeries_visits_b_3$Count, useNA = "ifany")
surgeries_visits_b_3$Count <- ifelse(is.na(surgeries_visits_b_3$Count),0,surgeries_visits_b_3$Count)

###

### cbind previous and new results

data_with_post_op <- rbind(surgeries_visits_3,surgeries_visits_b_3)
data_with_post_op <- reshape::rename(data_with_post_op,c(Count="num_post_op_visits"))
###-------------------------------------------------------actual code end

surgeries_14 <- surgeries_12
surgeries_14$surgery_date <- as.Date(as.POSIXct(surgeries_14$Visit, origin="1970-01-01"))

surgeries_14 <- merge(surgeries_14,surgeries_4[c("PatientProfileId","Visit","description_modifier_1_mid","description_modifier_2_mid","modifier")],
                      by=c("PatientProfileId","Visit"), all.x = T)

table(surgeries_14$modifier.x==surgeries_14$modifier.y, useNA = "ifany")

table(surgeries_14$description_modifier_1_mid=="Minimium Assist Surgeon", useNA = "ifany")
surgeries_14$description_modifier_1_mid <- ifelse(is.na(surgeries_14$description_modifier_1_mid),"a",surgeries_14$description_modifier_1_mid)
table(surgeries_14$description_modifier_1_mid, useNA = "ifany")
surgeries_14 <- subset(surgeries_14,description_modifier_1_mid!="Minimium Assist Surgeon",)

surgeries_14$remove <- ifelse((surgeries_14$PatientProfileId==18566 & surgeries_14$Count==2) | 
                                (surgeries_14$PatientProfileId==20449 & surgeries_14$Count==2) |
                                (surgeries_14$PatientProfileId==27957 & surgeries_14$Count==2) |
                                (surgeries_14$PatientProfileId==32973 & surgeries_14$Count==2) |
                                (surgeries_14$PatientProfileId==51207 & surgeries_14$Count==3),1,0)

table(surgeries_14$remove, useNA = "ifany")
surgeries_14 <- subset(surgeries_14,remove!=1,)



# surgeries_14$PatientProfileId <- as.numeric(surgeries_14$PatientProfileId)
# test <- sqldf("select a.* from surgeries_14 a left join surgeries_14 b
#               on (a.surgery_date = b.surgery_date and a.PatientProfileId = b.PatientProfileId)
#               where a.count!= b.count")
surgeries_14$modifier.y <- NULL
surgeries_14 <- reshape::rename(surgeries_14,c(modifier.x="modifier"))

a <- subset(surgeries_14,modifier=="Bilateral Procedure",c("PatientProfileId","CPTCode"))
surgeries_bilateral <- merge(a,surgeries_14[c("PatientProfileId","Visit","surgery_date")],by="PatientProfileId",all.x = T)
length(unique(a$PatientProfileId))
names(surgeries_bilateral)

surgeries_bilateral <- subset(surgeries_bilateral,,c("PatientProfileId","Visit","surgery_date","CPTCode"))
surgeries_bilateral$one_year_from_surgery <- surgeries_bilateral$surgery_date + 364


visits_bilateral <- visits
visits_bilateral$PatientProfileId <- as.numeric(visits_bilateral$PatientProfileId)

str(visits_bilateral$PatientProfileId)
str(surgeries_bilateral$PatientProfileId)

surgeries_bilateral$PatientProfileId <- as.numeric(surgeries_bilateral$PatientProfileId)

surgeries_bilateral <- merge(surgeries_bilateral,visits_bilateral[c("PatientProfileId","visit_date")],by="PatientProfileId", all.x = T)

surgeries_bilateral$count <- ifelse(surgeries_bilateral$visit_date > surgeries_bilateral$surgery_date &
                                      surgeries_bilateral$visit_date <= surgeries_bilateral$one_year_from_surgery,1,0)

surgeries_bilateral_1 <- surgeries_bilateral %>% group_by(PatientProfileId,surgery_date) %>% mutate(sum(count))

surgeries_bilateral_2 <- surgeries_bilateral_1
surgeries_bilateral_2$concat <- paste0(surgeries_bilateral_2$PatientProfileId,surgeries_bilateral_2$surgery_date)

surgeries_bilateral_2 <- surgeries_bilateral_2[!duplicated(surgeries_bilateral_2$concat),]

surgeries_bilateral_2$count <- NULL
surgeries_bilateral_2$concat <- NULL
table(surgeries_bilateral_2$CPTCode, useNA = "ifany")

str(surgeries_bilateral_2$CPTCode)
surgeries_bilateral_revision <- subset(surgeries_bilateral_2,CPTCode == "27487" | CPTCode == "27486",)

surgeries_bilateral_revision <- subset(surgeries_bilateral_2,CPTCode == "27487" | CPTCode == "27486",)

surgeries_bilateral_2 <- subset(surgeries_bilateral_2,CPTCode!="27486",)
surgeries_bilateral_2 <- subset(surgeries_bilateral_2,CPTCode!="27487",)

surgeries_bilateral_2$complication <- ifelse(surgeries_bilateral_2$PatientProfileId==8760,1,0)
bilateral_data_with_post_op <- surgeries_bilateral_2

bilateral_data_with_post_op <- reshape::rename(bilateral_data_with_post_op,c(`sum(count)`="num_post_op_visits"))


bilateral_data_with_post_op$failure <- ifelse(bilateral_data_with_post_op$num_post_op_visits >3 |
                                                bilateral_data_with_post_op$complication == 1,1,0)

test <- subset(bilateral_data_with_post_op,is.na(bilateral_data_with_post_op$failure),)

table(bilateral_data_with_post_op$failure, useNA = "ifany")
### --- cacl for non unique profile id cases 

surgeries_15 <- surgeries_12
surgeries_15 <- merge(surgeries_15,surgeries_4[c("PatientProfileId","Visit","code_modifier_1_mid","code_modifier_2_mid")],by=c("PatientProfileId","Visit"),all.x = T)
surgeries_15 <- subset(surgeries_15,code_modifier_1_mid!="81",)
surgeries_15 <- subset(surgeries_15,code_modifier_2_mid!="81",)

surgeries_15$code_modifier_1_mid <- NULL
surgeries_15$code_modifier_2_mid <- NULL

surgeries_15 <- subset(surgeries_15,modifier !="Bilateral Procedure",)
surgeries_15$surgery_date <- as.Date(as.POSIXct(surgeries_15$Visit, origin="1970-01-01"))
surgeries_15$one_year_from_surgery <- surgeries_15$surgery_date + 364
surgeries_15 <- surgeries_15 %>% arrange(PatientProfileId,surgery_date)
surgeries_15$Count <- NULL
surgeries_15 <- surgeries_15 %>% group_by(PatientProfileId) %>% mutate(Count=row_number())
surgeries_15 <- surgeries_15 %>% arrange(PatientProfileId,desc(Count))

surgeries_16 <- surgeries_15
surgeries_16 <- surgeries_16[!duplicated(surgeries_16$PatientProfileId),]
table(surgeries_16$CPTCode)
surgeries_16 <- subset(surgeries_16,CPTCode=="27446" | CPTCode == "27447",)
surgeries_16$new <- 1
surgeries_16 <- as.data.frame(surgeries_16)

surgeries_17 <- surgeries_15
surgeries_17 <- merge(surgeries_17,surgeries_16[c("PatientProfileId","Visit","new")],by=c("PatientProfileId","Visit"), all.x = T)
table(surgeries_17$new, useNA = "ifany")
surgeries_17$new <- ifelse(is.na(surgeries_17$new), 0,surgeries_17$new)
surgeries_17 <- subset(surgeries_17, new==0,)
surgeries_17 <- surgeries_17 %>% arrange(PatientProfileId,desc(Count))


surgeries_18 <- surgeries_17[!duplicated(surgeries_17$PatientProfileId),]
table(surgeries_18$CPTCode, useNA = "ifany")
surgeries_18 <- subset(surgeries_18,CPTCode=="27446" | CPTCode == "27447",)
surgeries_18$new <- 1

surgeries_19 <- surgeries_17
surgeries_19$new <- NULL
surgeries_19 <- merge(surgeries_19,surgeries_18[c("PatientProfileId","Visit","new")],by=c("PatientProfileId","Visit"), all.x = T)
table(surgeries_19$new, useNA = "ifany")
surgeries_19$new <- ifelse(is.na(surgeries_19$new), 0,surgeries_19$new)
surgeries_19 <- subset(surgeries_19, new==0,)
surgeries_19 <- surgeries_19 %>% arrange(PatientProfileId,desc(Count))


surgeries_20 <- surgeries_19[!duplicated(surgeries_19$PatientProfileId),]
table(surgeries_20$CPTCode, useNA = "ifany")
surgeries_20 <- subset(surgeries_20,CPTCode=="27446" | CPTCode == "27447",)
surgeries_20$new <- 1

surgeries_21 <- surgeries_19
surgeries_21$new <- NULL
surgeries_21 <- merge(surgeries_21,surgeries_20[c("PatientProfileId","Visit","new")],by=c("PatientProfileId","Visit"), all.x = T)
table(surgeries_21$new, useNA = "ifany")
surgeries_21$new <- ifelse(is.na(surgeries_21$new), 0,surgeries_21$new)
surgeries_21 <- subset(surgeries_21, new==0,)
surgeries_21 <- surgeries_21 %>% arrange(PatientProfileId,desc(Count))


surgeries_last_not_resurgery <- rbind(surgeries_16,surgeries_18,surgeries_20)
surgeries_last_not_resurgery <- surgeries_last_not_resurgery %>% group_by(PatientProfileId,surgery_date) %>% mutate(dup=row_number())
surgeries_last_not_resurgery <- subset(surgeries_last_not_resurgery, !(PatientProfileId == "27957" & dup == 2),)
surgeries_last_not_resurgery <- subset(surgeries_last_not_resurgery, !(PatientProfileId == "51207" & dup == 2),)
surgeries_last_not_resurgery <- subset(surgeries_last_not_resurgery, !(PatientProfileId == "32973" & dup == 1),)


surgeries_last_not_resurgery$new <- NULL
surgeries_last_not_resurgery$Count <- NULL
surgeries_last_not_resurgery$dup <- NULL



surgeries_last_not_resurgery_cor <- surgeries_last_not_resurgery %>% arrange(PatientProfileId,desc(surgery_date)) %>% group_by(PatientProfileId) %>% mutate(dup=row_number())
surgeries_last_not_resurgery_cor <- subset(surgeries_last_not_resurgery_cor, dup != 3,)

surgeries_last_not_resurgery_cor <- surgeries_last_not_resurgery_cor[!duplicated(surgeries_last_not_resurgery_cor$PatientProfileId),]

surgeries_last_not_resurgery_cor <- reshape::rename(surgeries_last_not_resurgery_cor,c(surgery_date="surgery_date_2"))

surgeries_last_not_resurgery <- merge(surgeries_last_not_resurgery, surgeries_last_not_resurgery_cor[c("PatientProfileId","surgery_date_2")], by = "PatientProfileId", all.x = T)



visits_2 <- visits[,c("PatientProfileId","visit_date")]
surgeries_last_not_resurgery_1 <- merge(surgeries_last_not_resurgery,visits_2,by="PatientProfileId",all.x = T)



surgeries_last_not_resurgery_1$count <- ifelse(surgeries_last_not_resurgery_1$surgery_date_2 > surgeries_last_not_resurgery_1$surgery_date &
                                                 surgeries_last_not_resurgery_1$surgery_date_2 < surgeries_last_not_resurgery_1$one_year_from_surgery,
                                               ifelse(surgeries_last_not_resurgery_1$visit_date > surgeries_last_not_resurgery_1$surgery_date &
                                                        surgeries_last_not_resurgery_1$visit_date < surgeries_last_not_resurgery_1$surgery_date_2,1,0),"Not yet filled")

table(surgeries_last_not_resurgery_1$count, useNA = "ifany")

surgeries_last_not_resurgery_1$count <- ifelse(surgeries_last_not_resurgery_1$count=="Not yet filled",
                                               ifelse(surgeries_last_not_resurgery_1$visit_date > surgeries_last_not_resurgery_1$surgery_date &
                                                        surgeries_last_not_resurgery_1$visit_date < surgeries_last_not_resurgery_1$one_year_from_surgery,1,0),surgeries_last_not_resurgery_1$count)

surgeries_last_not_resurgery_1$count <- as.numeric(surgeries_last_not_resurgery_1$count)     
table(is.na(surgeries_last_not_resurgery_1$visit_date), useNA = "ifany")

surgeries_last_not_resurgery_2 <- surgeries_last_not_resurgery_1 %>% group_by(PatientProfileId,surgery_date) %>% mutate(sum(count))
surgeries_last_not_resurgery_3 <- surgeries_last_not_resurgery_2
surgeries_last_not_resurgery_3$concat <- paste0(surgeries_last_not_resurgery_3$PatientProfileId,surgeries_last_not_resurgery_3$surgery_date)
surgeries_last_not_resurgery_3 <- surgeries_last_not_resurgery_3 %>% group_by(concat) %>% mutate(dup=row_number())
table(surgeries_last_not_resurgery_3$dup, useNA = "ifany")
surgeries_last_not_resurgery_3 <- surgeries_last_not_resurgery_3[!duplicated(surgeries_last_not_resurgery_3$concat),]

surgeries_last_not_resurgery_3$count <- NULL
surgeries_last_not_resurgery_3$dup <- NULL
surgeries_last_not_resurgery_3$concat <- NULL
surgeries_last_not_resurgery_3$visit_date <- NULL

surgeries_last_not_resurgery_3 <- reshape::rename(surgeries_last_not_resurgery_3,c(`sum(count)`="num_post_op_visits"))


###--------------------------------------------------------------------------------------------------------------------

table(surgeries_21$modifier, useNA = "ifany")
ids <- subset(surgeries_21, modifier == "surgery", c("PatientProfileId"))
ids <- as.vector(unique(ids))
ids <- ids$PatientProfileId

surgeries_22 <- surgeries_21[surgeries_21$PatientProfileId %in% ids,]
surgeries_23 <- surgeries_21[!surgeries_21$PatientProfileId %in% ids,]


names(surgeries_23)
surgeries_23$Count <- NULL
surgeries_23$new <- NULL
surgeries_23 <- surgeries_23 %>% arrange(PatientProfileId,surgery_date) %>% group_by(PatientProfileId) %>% mutate(dup=row_number())
surgeries_23$dup_1 <- surgeries_23$dup + 1

surgeries_23 <- merge(surgeries_23,surgeries_23[c("PatientProfileId","dup","surgery_date")], by.x = c("PatientProfileId","dup_1"),
                      by.y = c("PatientProfileId","dup"), all.x = T)

surgeries_23 <- reshape::rename(surgeries_23,c(surgery_date.x="surgery_date",surgery_date.y="surgery_date_2"))
table(is.na(surgeries_23$surgery_date_2),surgeries_23$CPTCode, useNA = "ifany")

visits_2 <- visits[,c("PatientProfileId","visit_date")]
surgeries_23 <- merge(surgeries_23,visits_2,by="PatientProfileId",all.x = T)


surgeries_23$count <- ifelse(surgeries_23$surgery_date_2 > surgeries_23$surgery_date &
                               surgeries_23$surgery_date_2 < surgeries_23$one_year_from_surgery,
                             ifelse(surgeries_23$visit_date > surgeries_23$surgery_date &
                                      surgeries_23$visit_date < surgeries_23$surgery_date_2,1,0),"Not yet filled")

table(surgeries_23$count, useNA = "ifany")

surgeries_23$count <- ifelse(surgeries_23$count=="Not yet filled",
                             ifelse(surgeries_23$visit_date > surgeries_23$surgery_date &
                                      surgeries_23$visit_date < surgeries_23$one_year_from_surgery,1,0),surgeries_23$count)
surgeries_23$count <- as.numeric(surgeries_23$count)

surgeries_23 <- surgeries_23 %>% group_by(PatientProfileId,surgery_date) %>% mutate(sum(count))
surgeries_23 <- surgeries_23 %>% distinct(PatientProfileId,surgery_date, .keep_all= TRUE)

test <- surgeries_23 %>% arrange(PatientProfileId,surgery_date) %>% group_by(PatientProfileId,surgery_date) %>% mutate(abc=row_number())
table(test$abc, useNA = "ifany")

surgeries_23 <- reshape::rename(surgeries_23,c(`sum(count)`="num_post_op_visits"))

surgeries_23 <- reshape::rename(surgeries_23,c(CPTCode.x="CPTCode",CPTCode.y="CPT_next_surgery"))

surgeries_24 <- subset(surgeries_23, CPTCode =="27486" | CPTCode =="27487" | CPTCode =="27488",)
surgeries_25 <- subset(surgeries_23, !(CPTCode =="27486" | CPTCode =="27487" | CPTCode =="27488"),)

surgeries_26 <- merge(surgeries_25,surgeries_24[c("PatientProfileId","surgery_date","modifier")], by = c("PatientProfileId","modifier"), all.x = T)
surgeries_26 <- reshape::rename(surgeries_26,c(surgery_date.x="surgery_date",surgery_date.y="surgery_date_revision_surgery"))
names(surgeries_26)

surgeries_26$abc <- ifelse(is.na(surgeries_26$surgery_date_revision_surgery), 0,1)
surgeries_26$abc_1 <- ifelse(surgeries_26$surgery_date_revision_surgery > surgeries_26$surgery_date, 1,0)
surgeries_26$abc_1 <- ifelse(is.na(surgeries_26$surgery_date_revision_surgery), 0,surgeries_26$abc_1)

surgeries_26$complications_revision_surgery <- ifelse(surgeries_26$abc==1 & surgeries_26$abc_1==1, 1,0)

surgeries_26 <- surgeries_26 %>% arrange(PatientProfileId, desc(complications_revision_surgery)) %>% distinct(PatientProfileId, dup, .keep_all = T)

surgeries_27 <- surgeries_26
names(surgeries_27)
keep <- c("PatientProfileId","modifier","Visit","CPTCode","surgery_date","num_post_op_visits","complications_revision_surgery")
surgeries_27 <- surgeries_27[keep]
table(surgeries_27$num_post_op_visits, useNA = "ifany")

###---------------------------------------------------------------------------------------------------------------------

###merging and combining data
master_1 <- surgeries_visits_3
master_2 <- surgeries_visits_b_3
master_3 <- bilateral_data_with_post_op
master_4 <- as.data.frame(surgeries_last_not_resurgery_3)
master_5 <- surgeries_27


master_1$complications_revision_surgery <- 0
master_1 <- reshape::rename(master_1,c(Count="num_post_op_visits"))
master_1$PatientProfileId <- as.character(master_1$PatientProfileId)
surgeries_4_1 <- surgeries_4
surgeries_4_1$PatientProfileId <- as.character(surgeries_4_1$PatientProfileId)
surgeries_4_1 <- subset(surgeries_4_1, PatientProfileId %in% master_1$PatientProfileId,)
master_1_1 <- merge(master_1,surgeries_4_1,by = "PatientProfileId", all.x = T)
library(bit64)
master_1_1$PatientProfileId <- as.integer64(master_1_1$PatientProfileId)


master_2$complications_revision_surgery <- 0
master_2 <- reshape::rename(master_2,c(Count="num_post_op_visits"))
surgeries_4_2 <- subset(surgeries_4, PatientProfileId %in% master_2$PatientProfileId,)
surgeries_4_2 <- subset(surgeries_4_2, !(CPTCode == "27486" | CPTCode == "27487" | CPTCode == "27488"),)
master_2_1 <- merge(master_2,surgeries_4_2,by = "PatientProfileId", all.x = T)


master_3$failure <- NULL
master_3$one_year_from_surgery <- NULL
master_3$visit_date <- NULL
master_3 <- reshape::rename(master_3,c(complication="complications_revision_surgery"))
master_3$modifier <- "Bilateral"
master_3 <- as.data.frame(master_3)


master_4$complications_revision_surgery <- 0
master_4$surgery_date_2 <- NULL
master_4$one_year_from_surgery <- NULL
master_4$surgery_date <- NULL
master_4_1 <- merge(master_4, surgeries_4, by = c("PatientProfileId","Visit","CPTCode"), all.x = T)
master_4_1$modifier.y <- NULL
master_4_1 <- reshape::rename(master_4_1,c(modifier.x="modifier"))


master_5$surgery_date <- NULL
master_5_1 <- merge(master_5, surgeries_4, by = c("PatientProfileId","Visit","CPTCode"), all.x = T)
master_5_1$modifier.y <- NULL
master_5_1 <- reshape::rename(master_5_1,c(modifier.x="modifier"))


test <- rbind(master_1_1,master_2_1,master_4_1,master_5_1)

test_1 <- subset(test,,c("PatientProfileId","PatientVisitDiagsId","PatientVisitId","Visit","CPTCode","ICD9Code"))

abc <- subset(PatientVisitDiags,,c("PatientVisitDiagsId","PatientVisitId","ICD9Code"))
abc$PatientVisitDiagsId <- as.numeric(abc$PatientVisitDiagsId)
abc <- subset(abc,ICD9Code=="996.4" | (ICD9Code>="996.40" & ICD9Code<="996.47") | ICD9Code=="996.49",)



df_fe <- test


##-------------------creating derived variables

#Age
df_fe$age <- floor(difftime(as.POSIXct(as.Date(df_fe$Visit, tz="UTC")), as.POSIXct(as.Date(df_fe$Birthdate, tz="UTC"), tz="UTC"), units="days")/365)
#BMI 
df_fe$bmi <- round((703*df_fe$weight)/((df_fe$height)^2),2)

head(df_fe$Visit)
head(df_fe$Birthdate)
head(df_fe$age)

# creating sucess col(main Y)

juily <- df_fe
# juily$sucess <- ifelse(df_fe$num_post_op_visits >5 | df_fe$complications_revision_surgery == 1 | !is.na(df$ICD9Code) , 0,1 )

table(juily$num_post_op_visits, useNA = "ifany")
juily$num_post_op_visits <- ifelse(is.na(juily$num_post_op_visits) ,0,juily$num_post_op_visits )
str(juily$complications_revision_surgery )
juily$sucess <- ifelse(juily$num_post_op_visits >6 | juily$complications_revision_surgery == 1,1,0)

juily$sucess <- ifelse(juily$ICD9Code %in% c("996.4" ,"996.40", "996.41", "996.43", "996.47", "996.49"),1,juily$sucess)

table(juily$sucess, useNA = "ifany")
###### Dimension Reduction ########

drops <- c("PId","PatientId","ProceduresId","PatientProfileId","PlaceOfServiceMId","PatientVisitProcsId","BatchId","OrderCodeId",
           
           "OrderId","CompanyId","PrimaryCareDoctorId","PID","OrderDiagId","MasterProceduresId","PRID","SDID","ICD9MasterDiagnosisId",
           
           "PatientVisitDiagsId","SPRID","USRID","ICD10MasterDiagnosisId","PatientRaceMid","DiagnosisId","TypeOfServiceMId","XID",
           
           "PatientVisitDiags1","PatientVisitDiags2","PatientVisitDiags3","PatientVisitDiags4","PatientVisitDiags5","PatientVisitDiags6",
           
           "PatientVisitDiags7","PatientVisitDiags8","PatientVisitDiags9","MaritalStatusMId","EmpStatusMId","SNOMEDMasterDiagnosisId",
           
           "FinancialClassMId","PatientStatusMId","TicketNumber","FacilityId.y","DateOfEntry","Code","code_place_of_service_mid","code_emp_status_mid",
           
           "code_financial_class_mid", "code_marital_status_mid","code_patient_race_mid","code_patient_status_mid","code_place_of_service_mid","Entered","BillStatus",
           
           "ClaimStatus","FirstFiledDate","LastFiledDate","TicketNumber","City","Zip","AddressType","Birthdate","DeathDate","ProfileNotes",
           
           "AlertNotes","BillingNotes","Inactive","CODE","allergy","RevenueCode","PRIORITY","ONSETDATE","QUALIFIER","DescriptionLong","Visit","PatientRaceId","df_fe",
           "num_post_op_visits","complications_revision_surgery","PatientVisitId","Modifier2MId","Modifier1MId","DateOfServiceFrom","DateOfServiceTo",
           "Modifier3MId","Modifier4MId","code_modifier_1_mid","code_modifier_2_mid","Country","NewPatient","Code.1","Description.1","ICD9Code",
           "code_type_of_service_mid","STOPREASON","STOPDATE","DESCRIPTION","description_modifier_2_mid","description_modifier_2_mid","remove","description_type_of_service_mid")


juily_1  <- juily[ , !(names(juily) %in% drops)]

df_final <- juily_1
median(df_final$height, na.rm = T)

df_final$height <- ifelse(is.na(df_final$height),median(df_final$height, na.rm = T),df_final$height)
table(is.na(df_final$weight), useNA = "ifany")
df_final$weight <- ifelse(is.na(df_final$weight),median(df_final$weight, na.rm = T),df_final$weight)

df_final$bmi <- round((703*df_final$weight)/((df_final$height)^2),2)

table(d$description_modifier_1_mid, useNA = "ifany")
sapply(df_final, function(x) sum(is.na(x)))


df_final <- subset(df_final,!is.na(description_modifier_1_mid),)
df_final <- subset(df_final,!is.na(description_marital_status_mid),)
df_final <- subset(df_final,!is.na(description_emp_status_mid),)

## EDA####

### Feature Engeering ###

## converting all chr cols to factor
str(df_final)
df_final <- df_final %>% mutate_if(is.character,as.factor)
df_final <- df_final %>% mutate_if(is.integer64,as.numeric)

df_final$age <- as.numeric(df_final$age)

df_final$sucess <- as.factor(df_final$sucess)
str(df_final)

## creating a Y ##

df_final$y <- df_final$sucess
df_final$sucess <- NULL

d <- df_final
str(d)
is.factor(d$y)

names(d)


d$DoctorId <- as.factor(d$DoctorId)
d$FacilityId.x <- as.factor(d$FacilityId.x)
d$CurrentCarrier <- as.factor(d$CurrentCarrier)
str(d)


## 75% of the sample size
smp_size <- floor(0.75 * nrow(d))

## set the seed to make your partition reproducible
set.seed(123)
d_train <- sample(seq_len(nrow(d)), size = smp_size)

train <- d[d_train, ]
test <- d[-d_train, ]



logit_model1 <- glm(y ~ ., data = train, family = "binomial")

train_glm_mdl2 <- subset(train,,c(2,5,17,18,22,58,60,69,78,19))

ncol(train)

logit_model2 <- glm(y ~ ., data = train[,c("y","Description","description_place_of_service_mid","DoctorId","description_patient_status_mid","description_emp_status_mid","description_marital_status_mid","age")], family = "binomial")



summary(logit_model1)
summary(logit_model2)

predictions<-predict(logit_model2, test,type = "response" )



table(train$description_place_of_service_mid, useNA = "ifany")
table(test$description_place_of_service_mid, useNA = "ifany")

test <- subset(test,!test$description_place_of_service_mid == "Office",)


combined_data<-cbind(test,predictions)


combined_data$response <- as.factor(ifelse(combined_data$predictions>0.5, 1, 0))
str(combined_data)
View(combined_data)

combined_data$response

library(caret)
library(e1071)
install.packages("e1071", dependencies = TRUE)

conf_matrix<-confusionMatrix(combined_data$response,combined_data$y)


conf_matrix


## ROC
install.packages("ROCR")
library(ROCR)
logit_scores <- prediction(predictions=combined_data$predictions, labels=combined_data$y)
logit_perf <- performance(logit_scores, "tpr", "fpr")

#plotting the ROC curve
plot(logit_perf,col = "darkblue",lwd=2,xaxs="i",yaxs="i",tck=NA, main="ROC Curve")
box()
abline(0,1, lty = 300, col = "green")
grid(col="aquamarine")

# Calculating the Area Under Curve (AUC)

logit_auc <- performance(logit_scores, "auc")
as.numeric(logit_auc@y.values)  ##AUC Value

#Calculating the KS Values

logit_ks <- max(logit_perf@y.values[[1]]-logit_perf@x.values[[1]])
logit_ks







################################################################################
## Creating Dummy Variables
################################################################################
# Here we want to create a dummy 0/1 variable for every level of a categorical
# variable
library(caret)
dummies <- dummyVars(y ~ ., data = d)            # create dummies for Xs
ex <- data.frame(predict(dummies, newdata = d))  # actually creates the dummies
names(ex) <- gsub("\\.", "", names(ex))          # removes dots from col names
d <- cbind(d$y, ex)                              # combine target var with Xs
names(d)[1] <- "y"                               # name target var 'y'
rm(dummies, ex)                                  # clean environment
################################################################################
# Identify Correlated Predictors and remove them
################################################################################
# If you build a model that has highly correlated independent variables it can
# lead to unstable models because it will tend to weight those more even though
# they might not be that important

# calculate correlation matrix using Pearson's correlation formula
descrCor <-  cor(d[,2:ncol(d)])                           # correlation matrix
highCorr <- sum(abs(descrCor[upper.tri(descrCor)]) > .85) # num Xs with cor > t
summary(descrCor[upper.tri(descrCor)])                    # summarize the cors

# which columns in your correlation matrix have a correlation greater than some
# specified absolute cutoff. Find them and remove them
highlyCorDescr <- findCorrelation(descrCor, cutoff = 0.85)
filteredDescr <- d[,2:ncol(d)][,-highlyCorDescr] # remove those specific columns
descrCor2 <- cor(filteredDescr)                  # calculate a new cor matrix
# summarize those correlations to see if all features are now within our range
summary(descrCor2[upper.tri(descrCor2)]) # you can specify a threshold for what is a high correlation

# update dataset by removing those filtered vars that were highly correlated
d <- cbind(d$y, filteredDescr)
names(d)[1] <- "y"

rm(filteredDescr, descrCor, descrCor2, highCorr, highlyCorDescr)  # clean up
################################################################################
# Identifying linear dependencies and remove them
################################################################################
# Find if any linear combinations exist and which column combos they are.
# Below I add a vector of 1s at the beginning of the dataset. This helps ensure
# the same features are identified and removed.
library(caret)
# first save response
y <- d$y

# create a column of 1s. This will help identify all the right linear combos
d <- cbind(rep(1, nrow(d)), d[2:ncol(d)])
names(d)[1] <- "ones"

# identify the columns that are linear combos
comboInfo <- findLinearCombos(d)
comboInfo

# remove columns identified that led to linear combos
d <- d[, -comboInfo$remove]

# remove the "ones" column in the first column
d <- d[, c(2:ncol(d))]

# Add the target variable back to our data.frame
d <- cbind(y, d)

rm(y, comboInfo)  # clean up
################################################################################
# Remove features with limited variation
################################################################################
# remove features where the values they take on is limited
# here we make sure to keep the target variable and only those input
# features with enough variation
nzv <- nearZeroVar(d, saveMetrics = TRUE)
d <- d[, c(TRUE,!nzv$zeroVar[2:ncol(d)])]

################################################################################
# Standardize (and/ normalize) your input features.
################################################################################
# Here we standardize the input features (Xs) using the preProcess() function
# by performing a min-max normalization (aka "range" in caret).

# Step 1) figures out the means, standard deviations, other parameters, etc. to
# transform each variable
preProcValues <- preProcess(d[,2:ncol(d)], method = c("range"))
# Step 2) the predict() function actually does the transformation using the
# parameters identified in the previous step. Weird that it uses predict() to do
# this, but it does!
d <- predict(preProcValues, d)

################################################################################
# Get the target variable how we want it for modeling with caret
################################################################################
# if greater than 50k make 1 other less than 50k make 0
d$y <- as.factor(ifelse(d$y==" >50K",1,0))
class(d$y)

# make names for target if not already made
levels(d$y) <- make.names(levels(factor(d$y)))
levels(d$y)

# levels of a factor are re-ordered so that the level specified is first and
# "X1" is what we are predicting. The X before the 1 has nothing to do with the
# X variables. It's just something weird with R. 'X1' is the same as 1 for the Y
# variable and 'X0' is the same as 0 for the Y variable.
d$y <- relevel(d$y,"X1")

################################################################################
# Data partitioning
################################################################################
set.seed(1234) # set a seed so you can replicate your results
library(caret)

# identify records that will be used in the training set. Here we are doing a
# 70/30 train-test split. You might modify this.
inTrain <- createDataPartition(y = d$y,   # outcome variable
                               p = .70,   # % of training data you want
                               list = F)
# create your partitions
train <- d[inTrain,]  # training data set
test <- d[-inTrain,]  # test data set

# down-sampled training set
dnTrain <- downSample(x=train[,2:ncol(d)], y=train$y)
a

################################################################################
# Specify cross-validation design
################################################################################
ctrl <- trainControl(method="cv",     # cross-validation set approach to use
                     number=3,        # k number of times to do k-fold
                     classProbs = T,  # if you want probabilities
                     summaryFunction = twoClassSummary, # for classification
                     #summaryFunction = defaultSummary,  # for regression
                     allowParallel=T)

################################################################################
# Train different models
################################################################################
# train a logistic regession on train set
sapply(train, function(x) sum(is.na(x)))
train <- subset(train,!is.na(description_code_patient_race_midAsian),)
names(train)

train <- reshape::rename(train,c(CPTCode27446="CPTCode_1",CPTCode27447="CPTCode_2"))
table(train$CPTCode27446, useNA = "ifany")








myModel1 <- train(y ~ .,               # model specification
                  data = train,        # train set used to build model
                  method = "glm",      # type of model you want to build
                  trControl = ctrl,    # how you want to learn
                  family = "binomial", # specify the type of glm
                  metric = "ROC"       # performance measure
)
myModel1

# train a logistic regession on down-sampled train set
myModel2 <- train(y ~ .,               # model specification
                  data = dnTrain,        # train set used to build model
                  method = "glm",      # type of model you want to build
                  trControl = ctrl,    # how you want to learn
                  family = "binomial", # specify the type of glm
                  metric = "ROC"       # performance measure
)
myModel2

# train a feed-forward neural net on train set
myModel3 <- train(y ~ .,               # model specification
                  data = train,        # train set used to build model
                  method = "nnet",     # type of model you want to build
                  trControl = ctrl,    # how you want to learn
                  tuneLength = 1:5,   # how many tuning parameter combos to try
                  maxit = 100,         # max # of iterations
                  metric = "ROC"       # performance measure
)
myModel3

# train a feed-forward neural net on the down-sampled train set using a customer
# tuning parameter grid
myGrid <-  expand.grid(size = c(10,15,20)     # number of units in the hidden layer.
                       , decay = c(.09,0.12))  #parameter for weight decay. Default 0.
myModel4 <- train(y ~ .,              # model specification
                  data = dnTrain,       # train set used to build model
                  method = "nnet",    # type of model you want to build
                  trControl = ctrl,   # how you want to learn
                  tuneGrid = myGrid,  # tuning parameter combos to try
                  maxit = 100,        # max # of iterations
                  metric = "ROC"      # performance measure
)
myModel4

################################################################################
# there are so many different types of models you can try, go here to see them all
# http://topepo.github.io/caret/available-models.html
################################################################################
# Capture the train and test estimated probabilities and predicted classes
# model 1
logit1_trp <- predict(myModel1, newdata=train, type='prob')[,1]
logit1_trc <- predict(myModel1, newdata=train)
logit1_tep <- predict(myModel1, newdata=test, type='prob')[,1]
logit1_tec <- predict(myModel1, newdata=test)
# model 2
logit2_trp <- predict(myModel2, newdata=dnTrain, type='prob')[,1]
logit2_trc <- predict(myModel2, newdata=dnTrain)
logit2_tep <- predict(myModel2, newdata=test, type='prob')[,1]
logit2_tec <- predict(myModel2, newdata=test)
# model 3
nn1_trp <- predict(myModel3, newdata=train, type='prob')[,1]
nn1_trc <- predict(myModel3, newdata=train)
nn1_tep <- predict(myModel3, newdata=test, type='prob')[,1]
nn1_tec <- predict(myModel3, newdata=test)
# model 4
nn2_trp <- predict(myModel4, newdata=dnTrain, type='prob')[,1]
nn2_trc <- predict(myModel4, newdata=dnTrain)
nn2_tep <- predict(myModel4, newdata=test, type='prob')[,1]
nn2_tec <- predict(myModel4, newdata=test)

################################################################################
# Now use those predictions to assess performance on the train set and testing
# set. Be on the lookout for overfitting
# model 1 - logit
(cm <- confusionMatrix(data=logit1_trc, train$y))
(testCM <- confusionMatrix(data=logit1_tec, test$y))
# model 2 - logit with down-sampled data
(cm2 <- confusionMatrix(data=logit2_trc, dnTrain$y))
(testCM2 <- confusionMatrix(data=logit2_tec, test$y))
# model 3 - nnet
(cm3 <- confusionMatrix(data=nn1_trc, train$y))
(testCM3 <- confusionMatrix(data=nn1_tec, test$y))
# model 4 - nnet with down-sampled data
(cm4 <- confusionMatrix(data=nn2_trc, dnTrain$y))
(testCM4 <- confusionMatrix(data=nn2_tec, test$y))



##########Modeling starts####################################################################################################################
d <- read.csv("/Users/anishpahwa/Desktop/em_save_TRAIN23.csv", stringsAsFactors = F)

################################################################################################################################################
######################################################################## Pre-Modellig Prep ############################################################
################################################################################################################################################
sapply(d, function(x) sum(is.na(x)))
d$X_WARN_ = NULL
d$failure <- ifelse(is.na(d$failure),0,d$failure)


######################################################################## Type Conversion  ############################################################
str(d)
d <- d %>% mutate_if(is.character,as.factor)
d <- d %>% mutate_if(is.integer,as.factor)
d <- d %>% mutate_if(is.numeric,as.factor)

d$age <- as.numeric(d$age)
d$IMP_bmi <- as.numeric(d$IMP_bmi)
d$IMP_height <- as.numeric(d$IMP_height)
d$IMP_weight <- as.numeric(d$IMP_weight)
################################################################################################################################################
######################################################################## Data Split ############################################################
################################################################################################################################################
set.seed(123)
## 75% of the sample size
smp_size <- floor(0.9 * nrow(d))

## set the seed to make your partition reproducible

d_train <- sample(seq_len(nrow(d)), size = smp_size)

train <- d[d_train, ]
test <- d[-d_train, ]
################################################################################################################################################
################################################################################################################################################

################################################################################################################################################
######################################################################## Modeling ############################################################
################################################################################################################################################

######################################################################## Random Forest ############################################################

library(randomForest)  
library(e1071)  
library(caret)  
library(ggplot2)  

rf_train <- train
rf_test <- test

rf <- randomForest(failure~., data=rf_train, ntree=200, proximity=T , mtry = 4, importance = T, replace = T)

rf_predictions<-predict(rf, rf_test,type = "prob" )


rf_combined_data<-cbind(rf_test,rf_predictions[,2])
rf_combined_data <- reshape::rename(rf_combined_data,c(`rf_predictions[, 2]`="prob"))

rf_combined_data$pred <- ifelse(rf_combined_data$prob > 0.10, 1, 0)
rf_combined_data$pred <- as.factor(rf_combined_data$pred)


print(confusionMatrix(data = rf_combined_data$pred,  
                      reference = rf_combined_data$failure))



var.imp = data.frame(importance(rf, type=2))

## ROC
#install.packages("ROCR")
library(ROCR)
logit_scores <- prediction(predictions=rf_combined_data$prob, labels=rf_combined_data$failure)
logit_perf <- performance(logit_scores, "tpr", "fpr")

#plotting the ROC curve
plot(logit_perf,col = "darkblue",lwd=2,xaxs="i",yaxs="i",tck=NA, main="ROC Curve")
box()
abline(0,1, lty = 300, col = "green")
grid(col="aquamarine")

# Calculating the Area Under Curve (AUC)

logit_auc <- performance(logit_scores, "auc")
as.numeric(logit_auc@y.values)  ##AUC Value

#Calculating the KS Values

logit_ks <- max(logit_perf@y.values[[1]]-logit_perf@x.values[[1]])
logit_ks


######################################################################## Logistic Regression ############################################################

## Data Prepration 

glm_train<- train
glm_train$IMP_description_code_patient_rac <- NULL
glm_train$FacilityId <- NULL

glm_test<- test
glm_test$IMP_description_code_patient_rac <- NULL
glm_test$FacilityId <- NULL
glm_test$prob <- NULL
glm_test$pred <- NULL
## Model training

logit_model1 <- glm(failure ~ ., data = glm_train, family = "binomial")

logit_model2 <- glm(failure ~ ., data = glm_train[,c("failure","DoctorId","age","IMP_bmi","description_fincial_class_mid","Sex","smoke","IMP_height","IMP_weight","bp" )], family = "binomial")

summary(logit_model1)
summary(logit_model2)

glm_predictions<-predict(logit_model2, test,type = "response" )

glm_combined_data<-cbind(glm_test,glm_predictions)


glm_combined_data$response <- as.factor(ifelse(glm_combined_data$glm_predictions>0.215, 1, 0))

library(caret)
library(e1071)
#install.packages("e1071", dependencies = TRUE)

(conf_matrix<-confusionMatrix(glm_combined_data$response,glm_combined_data$failure))





## ROC
install.packages("ROCR")
library(ROCR)
logit_scores <- prediction(predictions=combined_data$predictions, labels=combined_data$y)
logit_perf <- performance(logit_scores, "tpr", "fpr")

#plotting the ROC curve
plot(logit_perf,col = "darkblue",lwd=2,xaxs="i",yaxs="i",tck=NA, main="ROC Curve")
box()
abline(0,1, lty = 300, col = "green")
grid(col="aquamarine")

# Calculating the Area Under Curve (AUC)

logit_auc <- performance(logit_scores, "auc")
as.numeric(logit_auc@y.values)  ##AUC Value

#Calculating the KS Values

logit_ks <- max(logit_perf@y.values[[1]]-logit_perf@x.values[[1]])
logit_ks
