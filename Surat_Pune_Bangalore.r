system(paste("cd",getwd()))
system("mongoexport -h 52.77.156.120 -d delhivery_db -c ist -f conn.id,conn.cdn,conn.vh,conn.o,conn.d,s -q '{\"conn.o\":{$in:[\"Pune_Tathawde_H (Maharashtra)\",\"Surat_HUB (Gujarat)\"]}, \"conn.d\":\"Bengaluru_Bomsndra_HB (Karnataka)\",\"cs.sd\":{$gt:new Date(1472688000000),$lt:new Date(1475193600000)},\"conn.vh\":\"Srfc-Century Cargo STV BLR FTL\"}' --type=csv -o ist_sample.csv -u ro_express -p 'x[td7R%;,'") ## for surat-pune bangalore 
system("mongoexport -h 52.77.156.120 -d delhivery_db -c ist -f conn.id,conn.cdn,conn.vh,conn.o,conn.d,s -q '{\"conn.o\":{$in:[\"Pune_Tathawde_H (Maharashtra)\"]}, \"conn.d\":\"Bengaluru_Bomsndra_HB (Karnataka)\",\"cs.sd\":{$gt:new Date(1472688000000),$lt:new Date(1475193600000)},\"conn.vh\":\"Srfc-Saravana-PNQ BLR FTL\"}' --type=csv -o ist_samplepune.csv -u ro_express -p 'x[td7R%;,'")  ## for Pune-Bangalore 

system(paste0("python ",getwd(),"/ist_scan.py ",getwd()))
ist_saravana <- read.csv("ist_scannedpune.csv",stringsAsFactors = F) ## for pune-Bangalore

ist <- read.csv("ist_scanned.csv",stringsAsFactors = F) ##Surat-Bangalore

library(dplyr)
ist_saravana <- ist_saravana %>% filter(ss %in% c("Pending","Dispatched"))
ist_saravana <- ist_saravana %>% mutate(sd = as.POSIXct(paste(substring(sd,14,23),substring(sd,25,32)),format  = "%Y-%m-%d %H:%M:%S")) 

library(splitstackshape)
ist_saravana <- ist_saravana %>% group_by(conn.cdn,conn.id,conn.o,conn.d) %>% summarise(sd = paste(sd,collapse = ";")) %>% as.data.frame()
ist_saravana <- ist_saravana %>% cSplit("sd",sep = ";",direction = "wide",type.convert = F) %>% as.data.frame()
ist_saravana <- ist_saravana %>% select(conn.cdn,conn.id,conn.o,conn.d,dispatch = sd_1, pending = sd_2)
write.csv(ist_saravana,"pune_saravana.csv",row.names = F)

pune_ist <- ist %>% filter(grepl("Pune",conn.o)) %>% select(conn.o,conn.d,dispatch,pending,conn.cdn)
surat_ist <- ist %>% filter(grepl("Surat",conn.o)) %>% select(conn.o,conn.d,dispatch,pending,conn.cdn)

ist_match <-   merge(pune_ist,surat_ist,by = NULL) %>% mutate(dispatch_diff = as.numeric(difftime(dispatch.x,dispatch.y,units = "mins")), pending_diff =  abs(as.numeric(difftime(pending.y,pending.x,units = "mins")))) %>% arrange(abs(pending_diff),abs(dispatch_diff)) 
ist_match <- ist_match %>% filter(as.numeric(difftime(pending.x,dispatch.x)) > 11*60)
ist_match <- ist_match %>% group_by(conn.cdn.y) %>% summarise(pending_diff = min(pending_diff)) %>% as.data.frame() %>%  inner_join(ist_match) %>% as.data.frame() %>% arrange(desc(pending_diff)) 
ist_match <- ist_match %>% mutate(surat_blr_time = as.numeric(difftime(pending.y,dispatch.y,units = "mins"))/60,pune_blr_time = as.numeric(difftime(pending.x,dispatch.x,units = "mins"))/60, surat_pune_time = ) %>% arrange(desc(surat_blr_time)) 


surat_ist <- surat_ist %>% mutate(transit_time = as.numeric(difftime(pending,dispatch,units ="mins"))/60) %>% arrange(desc(transit_time))

pune_ist <- pune_ist %>% mutate(transit_time = as.numeric(difftime(pending,dispatch,units ="mins"))/60) %>% arrange(desc(transit_time))
write.csv(surat_ist,"surat_ist.csv",row.names = F)
write.csv(pune_ist,"pune_ist.csv",row.names = F)
write.csv(ist_match,"ist_match.csv",row.names = F)
