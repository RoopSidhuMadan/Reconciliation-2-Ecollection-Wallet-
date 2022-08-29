#!/usr/bin/env python
# coding: utf-8

# In[2]:


import numpy as np
import pandas as pd
import fsspec
from google.cloud import bigquery
from datetime import datetime
from datetime import date as d
import os
from google.oauth2 import service_account
from google.cloud import storage
from datetime import timedelta


# In[21]:


dates = d.today()
times = datetime.now()



#f = open('/home/sdlreco/crons/ybl_aeps/stat/stat-'+str(dates)+'.txt', 'a+')
#f.close()

def main():
    
    date = d.today()-timedelta(1)
    current_date5 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(1)
    current_date2 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(2)
    current_date3 = date.strftime('%d-%m-%Y')

    date = date.today()
    current_date4 = date.strftime('%d-%m-%Y')

    date = date.today()-timedelta(1)
    current_date6 = date.strftime('%Y%m%d')

    current_date = date.today()-timedelta(1)

    date = date.today()
    current_year = date.strftime('%Y')

    date = date.today()
    current_month = date.strftime('%m')

    date = date.today()
    current_day = date.strftime('%d')


    credentials = service_account.Credentials.from_service_account_file(key_path,scopes=["https://www.googleapis.com/auth/cloud-platform"])
    project_id = 'spicemoney-dwh'
    client = bigquery.Client(credentials=credentials, project=project_id, location='asia-south1')
   
    #fa=open('/home/sdlreco/crons/ybl_aeps/error/missing-'+str(date)+'.txt', 'w')
    #fa.close()
    
    #Specifying the path of the external file
    file_path = [str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/Axis_Axis-bank miis/SPICEMONEY_CDM_MIS'
                +str(current_date6)+'*.xlsx']
    
    # file_path= 'D:/axis-bank-mis-July-22.csv'

    print(file_path)
      #---------------------------------------------------------------------------------------------------------------------
    #Loading the external file into the database
    #---------------------------------------------------------------------------------------------------------------------
    schema = [{'name':'request_id','type':'STRING'},
              {'name':'cdm_card_num','type':'STRING'},
              {'name':'transaction_id','type':'STRING'},
              {'name':'transaction_amount','type':'FLOAT'},
              {'name':'transaction_date','type':'TIMESTAMP'},
              {'name':'fncl_posted_date','type':'TIMESTAMP'},
              {'name':'time_of_deposit','type':'STRING'}
              
              ]
                
    #Specifying the header column            
    header_list = ['request_id','cdm_card_num','transaction_id','transaction_amount','transaction_date','fncl_posted_date','time_of_deposit']
    
    list1= ['request_id','cdm_card_num','transaction_id']
    
    print('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/Axis_Axis-bank miis/SPICEMONEY_CDM_MIS'+str(current_date6)+'*.xlsx')
    # Reading data from excel to dataframe
    df = pd.read_excel('gs://sm-prod-rpa/'+str(current_year)+'/'+str(current_month)+'/'+str(current_day)+'/EmailReports/Axis_Axis-bank miis/SPICEMONEY_CDM_MIS'+str(current_date6)+'*.xlsx',skiprows=1,names=header_list,storage_options={"token": key_path},header=None,parse_dates = (['transaction_date', 'fncl_posted_date']) )             
    #df = pd.read_csv('axis-bank-mis-July-22.csv', skiprows=1, names=header_list, parse_dates = (['transaction_date', 'fncl_posted_date']), low_memory=False )
   
  
    df[list1]=df[list1].astype(str)
    print(df.dtypes)
    
    df.to_gbq(destination_table='sm_recon.wallet_axis_recharge_log', project_id='spicemoney-dwh', if_exists='replace' , table_schema = schema,credentials=credentials)
    print("Data moved to wallet_axis_recharge_log table")
    
    
    #---------------------------------------------------------------------------------------------------------------------
    #RECON OUTPUT - fetching data and forming table from AXIS_CDM,LIMIT_REPLENISHMENT,FTR, AXIS_CDM
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""select log_id as AXIS_CDM_LOGID,AXIS_BANK_MIS_TRANSACTION_ID,txn_date as TRANSACTION_DATE,AXIS_CDM_RESULT.client_id as CLIENTID,DMT_REPLENISHMENT_OUTPUT.TRANS_TYPE as TRANS_TYPE,agg_id as AGG_ID,processing_status as PROCESSING_STATUS,processing_desc as PROCESSING_DESC,DMT_REPLENISHMENT_STATUS,DMT_REPLENISHMENT_OUTPUT.reason as DMT_REPLENISHMENT_REASON,AXIS_BANK_MIS_CARD_NUM,SPICE_AMOUNT,
    coalesce(FTR_AMOUNT_TRANSFERRED,0) as FTR_AMOUNT_TRANSFERRED ,coalesce(DMT_REPLENISHMENT_AMOUNT,0) as DMT_REPLENISHMENT_AMOUNT,
    coalesce(AXIS_BANK_MIS_AMOUNT,0) as AXIS_BANK_MIS_AMOUNT,
    coalesce(SPICE_AMOUNT,0)-coalesce(FTR_AMOUNT_TRANSFERRED,0) as DIFF_AXIS_DETAIL_vs_FTR,
    coalesce(SPICE_AMOUNT,0)-coalesce(DMT_REPLENISHMENT_AMOUNT,0) as DIFF_AXIS_DETAIL_vs_DMT,
    coalesce(SPICE_AMOUNT,0)-coalesce(AXIS_BANK_MIS_AMOUNT,0) as DIFF_AXIS_DETAIL_vs_AXIS_BANK_MIS, from 
    (select log_id,ref_no,txn_date,sum(amount) as SPICE_AMOUNT ,client_id,agg_id,processing_status,processing_desc
    from `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and 
    Date(log_date_time)  between "2022-07-01" and "2022-07-31"
    group by log_id,ref_no,processing_desc,txn_date,client_id,agg_id,processing_status) as AXIS_CDM_RESULT
    FULL OUTER JOIN
    (select 
        t1.transfer_date as FTR_TRANSFER_DATE,
        t2.retailer_id AS FTR_CLIENT_ID,
        t1.unique_identification_no as FTR_UNIQUE_IDENTIFICATION_NO,
        sum(t1.amount_transferred) as FTR_AMOUNT_TRANSFERRED ,
        trans_type as TRANS_TYPE  
        FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
        JOIN spicemoney-dwh.prod_dwh.client_details as t2 ON t1.retailer_wallet_id=t2.ret_wallet_id
        where t1.comments in ('Wallet Recharge by Axis-CDM card') and
        DATE(t1.transfer_date)  between "2022-07-01" and "2022-07-31" 
        GROUP BY t1.UNIQUE_IDENTIFICATION_NO,FTR_CLIENT_ID,t1.transfer_date,TRANS_TYPE
    ) as FTR_RESULT
    on 
    AXIS_CDM_RESULT.log_id=FTR_RESULT.FTR_UNIQUE_IDENTIFICATION_NO
    LEFT OUTER JOIN
    (
     select  b.amount as DMT_REPLENISHMENT_AMOUNT,DMT_REPLENISHMENT_STATUS, identification_num  as DMT_REPLENISHMENT_IDENTIFICATION_NUM,b.reason as REASON,b.trans_type as TRANS_TYPE
        from 
    (
      select c.txn_date_time,c.status as DMT_REPLENISHMENT_STATUS,c.identificationnumber as DMT_REPLENISHMENT_IDENTIFICATION_NUM,c.reason
      from `spicemoney-dwh.prod_dwh.cme_log_limit_replenishment` c where 
      c.identificationnumber like ("%CDM%") and c.status ="Accept" and date(c.txn_date_time) between "2022-07-01" and "2022-07-31" 
      group by c.accept_date_time,c.status,c.identificationnumber,c.client_id,c.reason,c.txn_date_time
    ) a
    left outer join
    (
      select sum(a.amount) as amount,b.log_id as identification_num,b.agg_id as reason,txn_date_time,b.trans_type from 
      `spicemoney-dwh.prod_dwh.dmt_limit_replenishment` a
      JOIN `spicemoney-dwh.prod_dwh.cdm_request_logs` b
        on a.spice_tid = b.log_id  where spice_tid like '%CDM%' and date(txn_date_time)  between "2022-07-01" and "2022-07-31"
        group by b.agg_id,a.spice_tid,b.log_id,txn_date_time,b.trans_type 
    ) b
    on  DMT_REPLENISHMENT_IDENTIFICATION_NUM = identification_num
    ) as DMT_REPLENISHMENT_OUTPUT
    ON DMT_REPLENISHMENT_OUTPUT. DMT_REPLENISHMENT_IDENTIFICATION_NUM   =AXIS_CDM_RESULT.log_id
    LEFT outer join
    (
     select transaction_id as AXIS_BANK_MIS_TRANSACTION_ID,sum(transaction_amount) as AXIS_BANK_MIS_AMOUNT,transaction_date as AXIS_BANK_MIS_TRANSACTION_DATE,cdm_card_num as AXIS_BANK_MIS_CARD_NUM from (
                    select row_number() over ( PARTITION BY a.cdm_card_num,
                    extract(day from a.transaction_date),
                    extract(month from a.transaction_date),
                    extract(year from a.transaction_date),
                    extract(hour from a.transaction_date ), 
                    extract(minute from a.transaction_date ),
                    extract(second from a.transaction_date )
                    order by a.cdm_card_num
                   ) as ROW_NUM,a.* from `sm_recon.wallet_axis_recharge_log` a ) as tbl
        WHERE   tbl.ROW_NUM = 1 and DATE(transaction_date)  between "2022-07-01" and "2022-07-31"   group by transaction_id,transaction_date,cdm_card_num
    ) as AXIS_BANK_MIS_RESULT
    ON AXIS_BANK_MIS_RESULT.AXIS_BANK_MIS_TRANSACTION_ID=AXIS_CDM_RESULT.ref_no  """
    
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_ftr_limit_cdm_recharge', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_ftr_limit_cdm_recharge table")
    
    #---------------------------------------------------------------------------------------------------------------------
    #limit detail
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""
        select "CDM" as Service,"Axis Card ( Per Card) active" as Aggregator,MIN_TRANSACTION_LOG_ID,MIN_TRANSACTION_AMOUNT,
        MAX_TRANSACTION_LOG_ID,MAX_TRANSACTION_AMOUNT , Number_of_Transaction_Count_Limit_Per_Card_Max,Transaction_Amt_Limit_per_card_Max from 
        (
        SELECT log_id as MIN_TRANSACTION_LOG_ID,amount as MIN_TRANSACTION_AMOUNT,processing_status,log_date_time FROM `spicemoney-dwh.prod_dwh.cdm_request_logs` 
        WHERE amount=(SELECT MIN(amount) FROM `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and Date(log_date_time) =@date) and agg_id = 'AXIS' and processing_status="S" and Date(log_date_time)=@date limit 1
        ) as t1,
        (
        SELECT log_id as MAX_TRANSACTION_LOG_ID,amount as MAX_TRANSACTION_AMOUNT FROM `spicemoney-dwh.prod_dwh.cdm_request_logs` 
        WHERE amount=(SELECT MAX(amount) FROM `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and Date(log_date_time)=@date ) and agg_id = 'AXIS' and processing_status="S" and Date(log_date_time) =@date  limit 1
        ) as t2,
        (
        select max(Number_of_transaction_On_Card) as Number_of_Transaction_Count_Limit_Per_Card_Max  from 
        (SELECT card_no,count(*) as Number_of_transaction_On_Card  FROM `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and Date(log_date_time) =@date  group by card_no) as row_count
        ) as t3,
        (select max(Transaction_Amt_Limit_per_card_Max) as Transaction_Amt_Limit_per_card_Max  from 
        (SELECT card_no,sum(amount) as Transaction_Amt_Limit_per_card_Max  FROM `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and Date(log_date_time) =@date  group by card_no) as row_count

        )"""
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_limit_detail', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_limit_detail table")
    
    #---------------------------------------------------------------------------------------------------------------------
    #FTR Summary
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""
    select DATE(transfer_date) as Transfer_Date,
    comments as Comments,
     trans_type as Trans_Type,
    sum(amount_transferred) as Amount_Transferred,
    FROM spicemoney-dwh.prod_dwh.cme_wallet
    where comments IN ('Wallet Recharge by Axis-CDM card','Axis-CDM card charge','Axis-CDM card charge-reversed') and 
    DATE(transfer_date) = @date
    GROUP BY comments, transfer_date,trans_type;
    """
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_ftr_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_ftr_summary table")
   
    #---------------------------------------------------------------------------------------------------------------------
    # Limit replenishment by sdl summary
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""select  sum(b.amount) as AMOUNT,DMT_REPLENISHMENT_STATUS, b.reason as REASON
            from 
        (
          select c.txn_date_time,c.status as DMT_REPLENISHMENT_STATUS,c.identificationnumber as DMT_REPLENISHMENT_IDENTIFICATION_NUM
          from `spicemoney-dwh.prod_dwh.cme_log_limit_replenishment` c where 
          c.identificationnumber like ("%CDM%") and c.status ="Accept" and date(c.txn_date_time) = @date 
          group by c.accept_date_time,c.status,c.identificationnumber,c.client_id,c.reason,c.txn_date_time
        ) a
        join
        (
          select sum(a.amount) as amount,b.log_id as identification_num,b.agg_id as reason,txn_date_time,b.trans_type from 
          `spicemoney-dwh.prod_dwh.dmt_limit_replenishment` a
          JOIN `spicemoney-dwh.prod_dwh.cdm_request_logs` b
            on a.spice_tid = b.log_id  where spice_tid like '%CDM%' and date(txn_date_time) = @date  
            and b.agg_id="AXIS"
            group by b.agg_id,a.spice_tid,b.log_id,txn_date_time,b.trans_type 
        ) b
        on DMT_REPLENISHMENT_IDENTIFICATION_NUM = identification_num

        group by DMT_REPLENISHMENT_STATUS,REASON;"""
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_limit_replenishment_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_limit_replenishment_summary table")
    
     #---------------------------------------------------------------------------------------------------------------------
    #AXIS detail log
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query= """select  Date(log_date_time) as txn_date,sum(amount) as SPICE_AMOUNT,agg_id,processing_status,processing_desc
    from `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and Date(log_date_time)=@date
    group by agg_id,processing_status,processing_desc,txn_date
    """
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_detail_log', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_detail_log table")
    
     #---------------------------------------------------------------------------------------------------------------------
    #Axis Bank MIS summary
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""select sum(AXIS_BANK_MIS_AMOUNT) as TRANSACTIONAMOUNT,date(transaction_date) as TRANSACTIONDATE from (
    select transaction_id as AXIS_BANK_MIS_TRANSACTION_ID,sum(transaction_amount) as AXIS_BANK_MIS_AMOUNT,transaction_date,cdm_card_num as AXIS_BANK_MIS_CARD_NUM from (
                select row_number() over ( PARTITION BY a.cdm_card_num,
                extract(day from a.transaction_date),
                extract(month from a.transaction_date),
                extract(year from a.transaction_date),
                extract(hour from a.transaction_date ), 
                extract(minute from a.transaction_date ),
                extract(second from a.transaction_date )
                order by a.cdm_card_num
               ) as ROW_NUM,a.* from `sm_recon.wallet_axis_recharge_log` a ) as tbl
    WHERE   tbl.ROW_NUM = 1 and DATE(transaction_date) =@date    group by transaction_id,transaction_date,cdm_card_num) group by (TRANSACTIONDATE)"""
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_bank_mis_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_bank_mis_summary table")
    
     #---------------------------------------------------------------------------------------------------------------------
    #FTR Summary
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""select DATE(transfer_date) as Transfer_Date,
    comments as Comments,
     trans_type as Trans_Type,
    sum(amount_transferred) as Amount_Transferred,
    FROM spicemoney-dwh.prod_dwh.cme_wallet
    where comments IN ('Wallet Recharge by Axis-CDM card','Axis-CDM card charge','Axis-CDM card charge-reversed') and 
    DATE(transfer_date) = @date
    GROUP BY comments, transfer_date,trans_type;
    """
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_ftr_summary', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
   # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_ftr_summary table")
    
     #---------------------------------------------------------------------------------------------------------------------
    #RECON TRACKER OUTPUT
    #--,coalesce(Axis_bank_statement_CR_amt,0) as Axis_bank_statement_CR_amt 
    # --, coalesce(Net_Spice_Detail_Logs_Amount,0)-coalesce(Axis_bank_statement_CR_amt,0)  as Diff_Axis_cdm_detail_logs_amt_vs_bank_statement_sum_amt 
    #---------------------------------------------------------------------------------------------------------------------
    
    sql_query="""select Transaction_Date,Txn_Count_As_Per_Axis_CDM_Logs,
    Txns_Amt_As_Per_Axis_CDM_Logs_S,
    Txns_Amt_As_Per_Axis_CDM_Logs_F,
    Net_Spice_Detail_Logs_Amount,
    FTR_AMOUNT_TRANSFERRED as  Sum_Of_Amount_As_Per_FTR_Report_CDM_Txns,Sum_Of_Amount_As_Per_Limit_Replenishment_Report_Accept,Net_Amount_Credited_In_Agent_Wallet,SUM_OF_AMOUNT_BANK_MIS,
    coalesce(Txns_Amt_As_Per_Axis_CDM_Logs_S,0)-coalesce(Net_Amount_Credited_In_Agent_Wallet,0)  as  Diff_Sdl_Detail_vs_FTR_Limit_Replenishment_Report ,
    coalesce(Net_Spice_Detail_Logs_Amount,0)-coalesce(SUM_OF_AMOUNT_BANK_MIS,0)  as  Diff_Axis_cdm_detail_logs_amt_vs_bank_mis_sum_amt 
    from 
    (
    select Transaction_Date,Txn_Count_As_Per_Axis_CDM_Logs,
    Txns_Amt_As_Per_Axis_CDM_Logs_S,
    Txns_Amt_As_Per_Axis_CDM_Logs_F,
    coalesce(Txns_Amt_As_Per_Axis_CDM_Logs_S,0)+coalesce(Txns_Amt_As_Per_Axis_CDM_Logs_F,0) as  Net_Spice_Detail_Logs_Amount,
    coalesce(FTR_AMOUNT_TRANSFERRED,0) as FTR_AMOUNT_TRANSFERRED,
    coalesce(DMT_REPLENISHMENT_AMOUNT,0) as  Sum_Of_Amount_As_Per_Limit_Replenishment_Report_Accept,
    coalesce(FTR_AMOUNT_TRANSFERRED,0)+coalesce(DMT_REPLENISHMENT_AMOUNT,0) as  Net_Amount_Credited_In_Agent_Wallet,
    coalesce(AXIS_BANK_MIS_AMOUNT,0) as SUM_OF_AMOUNT_BANK_MIS,
    from 
    (select count(*) as  Txn_Count_As_Per_Axis_CDM_Logs ,Date(log_date_time) as Transaction_Date,
    from `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and 
    Date(log_date_time) = @date group by Transaction_Date ) as t1,
    (select sum(amount) as  Txns_Amt_As_Per_Axis_CDM_Logs_S 
    from `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and 
    Date(log_date_time) = @date and processing_status="S") as t2,
    (select sum(amount) as  Txns_Amt_As_Per_Axis_CDM_Logs_F 
    from `spicemoney-dwh.prod_dwh.cdm_request_logs` where agg_id = 'AXIS' and 
    Date(log_date_time) = @date and processing_status="F") as t3,
    (
    select 

        sum(t1.amount_transferred) as FTR_AMOUNT_TRANSFERRED
        FROM spicemoney-dwh.prod_dwh.cme_wallet as t1
        where t1.comments in ('Wallet Recharge by Axis-CDM card') and
        DATE(t1.transfer_date) =@date 

    )as t4,
    (
      select  sum(b.amount) as DMT_REPLENISHMENT_AMOUNT
          from 
      (
        select c.txn_date_time,c.status as DMT_REPLENISHMENT_STATUS,c.identificationnumber as DMT_REPLENISHMENT_IDENTIFICATION_NUM,c.reason
        from `spicemoney-dwh.prod_dwh.cme_log_limit_replenishment` c where 
        c.identificationnumber like ("%CDM%") and c.status ="Accept" and date(c.txn_date_time)=@date
        group by c.accept_date_time,c.status,c.identificationnumber,c.client_id,c.reason,c.txn_date_time
      ) a
      left outer join
      (
        select sum(a.amount) as amount,b.log_id as identification_num,b.agg_id as reason,txn_date_time,b.trans_type from 
        `spicemoney-dwh.prod_dwh.dmt_limit_replenishment` a
        JOIN `spicemoney-dwh.prod_dwh.cdm_request_logs` b
          on a.spice_tid = b.log_id  where spice_tid like '%CDM%' and date(txn_date_time) =@date 
          group by b.agg_id,a.spice_tid,b.log_id,txn_date_time,b.trans_type 
      ) b
      on  DMT_REPLENISHMENT_IDENTIFICATION_NUM = identification_num

    )as t5,
    (
        select sum(transaction_amount) as AXIS_BANK_MIS_AMOUNT from (
                      select row_number() over ( PARTITION BY a.cdm_card_num,
                      extract(day from a.transaction_date),
                      extract(month from a.transaction_date),
                      extract(year from a.transaction_date),
                      extract(hour from a.transaction_date ), 
                      extract(minute from a.transaction_date ),
                      extract(second from a.transaction_date )
                      order by a.cdm_card_num
                    ) as ROW_NUM,a.* from `sm_recon.wallet_axis_recharge_log` a ) as tbl
          WHERE   tbl.ROW_NUM = 1 and DATE(transaction_date) = @date
    ) as t6)"""
    
    job_config = bigquery.QueryJobConfig(destination='spicemoney-dwh.sm_recon.wallet_axis_recon_output', write_disposition='WRITE_TRUNCATE' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])
    # job_config2 = bigquery.QueryJobConfig(destination='spicemoney-dwh.prod_sm_recon.prod_wallet_ftr_ecollect_ybl_recharge', write_disposition='WRITE_APPEND' ,  query_parameters=[bigquery.ScalarQueryParameter("date", "DATE" , current_date)])

    query_job = client.query(sql_query, job_config=job_config)
    #query_job = client.query(sql_query, job_config=job_config2)

    results = query_job.result()

    print("Data moved to wallet_axis_recon_output table")
    
main()






# In[ ]:




