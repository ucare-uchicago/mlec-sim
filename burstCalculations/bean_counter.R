# A code to compute data loss probability for bursts. Last update on September 2022, serkay olmez 
# see simplified-combinatorics-of-burst-with-multi-layer-EC.pdf for details
library(DT);library(ggplot2);require(tidyr); library(scales);library(dplyr)
library(MASS);library(plotly);
library(htmlwidgets)

# parts of the code is hard coded. Use for  outer_k+outer_m=3 only
drivelabel <- function(i) { paste0("grp",1+floor((i)/inner_size), '_drv', 1+i %% (inner_size)  )}

t1 <- list(family = "Times New Roman",color = "black")
m <- list( l = 120, r = 90, b = 100, t = 100, pad = 4)

calculateThis<-  function(inner_k,inner_m,outer_k,outer_m) { 
  inner_size<-inner_k+inner_m  ;outer_size<-outer_k+outer_m
  minFailure=(inner_m+1)*(outer_m+1); Ndrives<-inner_size*outer_size
  print(paste0("inner_k:",inner_k,"; inner_m:",inner_m, ";outer_k:",outer_k,"; outer_m:",outer_m))
  df<-data.frame('uid'=seq(0,(2^Ndrives-1),1)) # create unique id for all combinations. Ndrives --> 2 levels each; 2^Ndrives possible combinations
  
  for(i in c(0: (Ndrives-1)  )){df[,drivelabel(i) ]<-floor((df$uid %% 2^(i+1)) /2^(i) )} # create binary labels for drive status. 0 for healthy; 1 for failed
  # also group the drives into EC grps
  for(i in c(0:(outer_size-1) )){  # add up the failures in each grp
    grpf_label<-paste0('grp',i+1,'f')
    for(j in c(0:(inner_size-1) )){
       if(j==0){ df[,grpf_label]<-0}
      #print(drivelabel(i*inner_size+j))
      df[,grpf_label]<-df[,grpf_label]+df[,drivelabel(i*inner_size+j)]
    }
    
  }
  
  for(i in c(0:(outer_size-1) )){ # a grp fails if it has more failures than inner_m
     grpf_label<-paste0('grp',i+1,'f')
     grpf_DLlabel<-paste0('grp',i+1,'_DL')
     df[,grpf_DLlabel]<-0
     df[df[,grpf_label]>inner_m  ,grpf_DLlabel]<-1
  } 
  
  
  # counting the total failed drives
  df$totalDF<-df[,drivelabel(0)]
  for(i in c(1:(Ndrives-1) )){
    drvid<-drivelabel(i)
    df$totalDF<-df$totalDF+df[,drvid]
  }
  
  
  for(i in c(0:(outer_size-1) )){# counting how many inner grps are lost
    grpf_DLlabel<-paste0('grp',i+1,'_DL')
    if(i==0){ df[,'groupsLOST']<-0}
    df[,'groupsLOST']<- df[,'groupsLOST']+ df[, grpf_DLlabel]
  }
  
  df$dataLost<-0
  df$dataLost[df[,'groupsLOST']>outer_m]<-100  # data is lost if # of grps lost > outer parity
  df_agg<- as.data.frame(df %>% group_by(totalDF)%>%summarise(ndata = n(),dataLost_pct = mean(dataLost)))
  
  
  df_th<-data.frame('totalDF'=seq(0,Ndrives,1) ) # preparing for the theoretical calculations
  
  hundredreached=0; # use this to terminate the calculation; if prob of data loss=100% for x failed drives, it will certainly be 100% for x+1 failed drives.
  if(outer_size==3){ ## hard coded loops for now for outer EC size=3;  will make it generic later.
    
    thv=rep(0,minFailure)
    simv=rep(0,minFailure);  thisFC <-minFailure
     for(thisFC in c(minFailure:Ndrives )){  # start the calc from mininum number of failures to lose data
       
       if(hundredreached==0){
       dffc<-df[df$totalDF==thisFC,]
       
      df_aggFullsum<- as.data.frame(dffc %>% group_by(totalDF)%>%summarise(ndata = n(),dataLost_pct = mean(dataLost)))
     #print(paste0("df_aggx$dataLost_pct[1]:",df_aggFullsum$dataLost_pct[1]))
     
     
     totalprobDL=0
     totalprob=0
     combintotal=0
     f0max=min(inner_size,thisFC)
    for(f0 in c(0:f0max )){  
      
      f1m=min(thisFC-f0,inner_size)
      for(f1 in c(0:f1m )){ 
       
        fgroups=0;dataloss<-0; f2=thisFC-f0-f1
        if(f0>inner_m){fgroups<-fgroups+1};if(f1>inner_m){fgroups<-fgroups+1}; if(f2>inner_m){fgroups<-fgroups+1}
        if(fgroups>outer_m){dataloss<-1}
       
        ft=f0+f1+f2
        if(f2<=inner_size ){
          combin=factorial(inner_size)^outer_size/(factorial(f0)*factorial(inner_size-f0)* factorial(f1)*factorial(inner_size-f1)* factorial(f2)*factorial(inner_size-f2))
          norm=factorial(Ndrives)/( factorial(Ndrives-thisFC)* factorial(thisFC))
          totalprobDL=totalprobDL+dataloss*combin/norm;  totalprob=totalprob+combin/norm;  combintotal=combintotal+combin;
          
          dfs=dffc[dffc$grp1f==f0 & dffc$grp2f==f1 & dffc$grp3f==f2,]
          df_aggx<- as.data.frame(dfs %>% group_by(totalDF)%>%summarise(ndata = n(),dataLost_pct = mean(dataLost)))
          nrowcounted=nrow(dfs)
          
          #print(paste0('thisFC:',thisFC,  '--->f0:',f0,", f1:",f1,",f2:",f2, ', ft:',ft, ',fgroups:',fgroups,', dataloss:',dataloss, ", prob:",combin/norm, ", combin:",combin, ',  nrowcounted:',nrowcounted, ", avg loss:", df_aggx$dataLost_pct[1]))
        }
      }
      
    }
    if(totalprobDL==1){ hundredreached=1}
       }else{totalprobDL=1}
     thv<-c(thv,100*totalprobDL)
     simv <-c(simv,df_aggFullsum$dataLost_pct[1])
            #print(paste0('thisFC:',thisFC, '; totalprobDL:',100*totalprobDL, "%")); print(paste0('totalprob:',100*totalprob, "%"))
  
            
  }
    
    
  }
  df_th$dataLost_pct<-thv
  df_th$counted  <-simv
  #f0=3; f1=1;f2=2;dffc[dffc$grp1f==f0 & dffc$grp2f==f1 & dffc$grp3f==f2,]
  
df_th$inner_k<-inner_k; df_th$inner_m<-inner_m; 
df_th$outer_k<-outer_k; df_th$outer_m<-outer_m; 
  
df_th
}


inner_k<-5; inner_m<-0; 
outer_k<-2; outer_m<-1; 
returnedData= calculateThis(inner_k,inner_m,outer_k,outer_m)
figT <- plot_ly() %>% 
  add_trace(x = returnedData$totalDF, y = returnedData$counted, type = 'scatter',mode = "markers", name=paste0(' EC:(',outer_k,"+",outer_m,")/(",inner_k,"+",inner_m,")" ),
                marker = list( symbol='star', size = 16)) %>%
  add_trace(x = returnedData$totalDF, y = returnedData$dataLost_pct, type = 'scatter',mode = "markers", name="Formula",showlegend = F) 


inner_k<-5; inner_m<-1; 
outer_k<-2; outer_m<-1; 
returnedData= calculateThis(inner_k,inner_m,outer_k,outer_m)
figT2 <- plot_ly() %>% 
  add_trace(x = returnedData$totalDF, y = returnedData$counted, type = 'scatter',mode = "markers", name=paste0(' EC:(',outer_k,"+",outer_m,")/(",inner_k,"+",inner_m,")" ),
                marker = list( symbol='star', size = 16)) %>%
  add_trace(x = returnedData$totalDF, y = returnedData$dataLost_pct, type = 'scatter',mode = "markers", name="Formula",showlegend = F) 

inner_k<-5; inner_m<-2; 
outer_k<-2; outer_m<-1; 
returnedData= calculateThis(inner_k,inner_m,outer_k,outer_m)
figT3 <- plot_ly() %>% 
  add_trace(x = returnedData$totalDF, y = returnedData$counted, type = 'scatter',mode = "markers", name=paste0(' EC:(',outer_k,"+",outer_m,")/(",inner_k,"+",inner_m,")" ),
                marker = list( symbol='star', size = 16)) %>%
  add_trace(x = returnedData$totalDF, y = returnedData$dataLost_pct, type = 'scatter',mode = "markers", name="Formula",showlegend = F) 


inner_k<-5; inner_m<-0; 
outer_k<-3; outer_m<-0; 
returnedData= calculateThis(inner_k,inner_m,outer_k,outer_m)
figT4 <- plot_ly() %>% 
  add_trace(x = returnedData$totalDF, y = returnedData$counted, type = 'scatter',mode = "markers", name=paste0(' EC:(',outer_k,"+",outer_m,")/(",inner_k,"+",inner_m,")" ),
                marker = list( symbol='star', size = 16)) %>%
  add_trace(x = returnedData$totalDF, y = returnedData$dataLost_pct, type = 'scatter',mode = "markers", name="Formula",showlegend = F) 


inner_k<-5; inner_m<-1; 
outer_k<-3; outer_m<-0; 
returnedData= calculateThis(inner_k,inner_m,outer_k,outer_m)
figT5 <- plot_ly() %>% 
  add_trace(x = returnedData$totalDF, y = returnedData$counted, type = 'scatter',mode = "markers", name=paste0(' EC:(',outer_k,"+",outer_m,")/(",inner_k,"+",inner_m,")" ),
                marker = list( symbol='star', size = 16)) %>%
  add_trace(x = returnedData$totalDF, y = returnedData$dataLost_pct, type = 'scatter',mode = "markers", name="Formula",showlegend = F) 

inner_k<-5; inner_m<-2; 
outer_k<-3; outer_m<-0; 
returnedData= calculateThis(inner_k,inner_m,outer_k,outer_m)
figT6 <- plot_ly() %>% 
  add_trace(x = returnedData$totalDF, y = returnedData$counted, type = 'scatter',mode = "markers", name=paste0(' EC:(',outer_k,"+",outer_m,")/(",inner_k,"+",inner_m,")" ),
                marker = list( symbol='star', size = 16)) %>%
  add_trace(x = returnedData$totalDF, y = returnedData$dataLost_pct, type = 'scatter',mode = "markers", name="Formula",showlegend = F) 


fig <- subplot(figT, figT2,figT3,figT4, figT5,figT6, nrows = 3, shareX = TRUE) 
fig%>%
  layout(
    title = paste0('Probability of data loss vs number of drives failing ' )   ,
    xaxis = list(range = list(0,Ndrives),title="Number of failed drives",font=t1,dtick=1,tick0=0,tickmode="linear",tickfont=list(size=15),hoverformat='.2f'),
    yaxis = list(title = "Probability of Data Loss(%)",font = t1,tickmode = "array",tickfont = list(size = 15), hoverformat= '.2f',tick0 = 0.02)
   ) %>% 
  layout(paper_bgcolor='rgba(0,0,0,0)',paper_bgcolor='rgba(0,0,0,0)')%>% layout(showlegend = T) %>%
  layout(autosize = F, width = 1000, height = 800,margin=m)
  
#figT%>% config(mathjax = "cdn")

#for (i in seq(1,length(returnedData$totalDF))) {
#  print(paste0(' ', returnedData$totalDF[i], ' ', returnedData$dataLost_pct[i], ' '))
#}
