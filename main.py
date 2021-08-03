#!/usr/bin/env python
# coding: utf-8

# log writer
def writer(filename, words):
    with open(filename + '.txt', mode='w') as writer:
        content = writer.write(f'{words} \n')


# Log appender
def appender(filename=0, words=0, var=''):
    with open(filename + '.txt', mode='a') as writer:
        content = writer.write(f'{words} {var} \n')

import psycopg2
import pandas as pd
import numpy as np
import pathlib
import dask
import dask.dataframe as dd
import os
import matplotlib.pyplot as plt
import seaborn as sns
from dateutil.relativedelta import relativedelta
from datetime import datetime
from datetime import date
writer('churn_log', '************************PROGRAM STARTED*************************************')
appender('churn_log', 'Import done')

def read_sql_query_dwh(script):
    conn = psycopg2.connect("dbname=data_warehouse host=flwdatawarehouse.cl7qyipyw37a.eu-west-2.redshift.amazonaws.com "
                            "port=5439 user=flw_warehouse password=t2vgvRd2wOh")
    dfs = pd.read_sql_query(script,conn)
    conn.close()
    return dfs

# Connection to the DWH
nuban = read_sql_query_dwh('''

select * from (
select  concat(concat(firstname,'  '),LastName) customer_name, cf.Nuban, 
trunc(dategenerated) DateCreated,
sum(case when tw.DebitCreditIndicator ='D' then transactionamount  else 0 end)Total_Debit,
sum(case when tw.DebitCreditIndicator ='C' then transactionamount else 0 end)Total_Credit,
(select currentbalance from custom.nuban where wemaaccount = cf.nuban) CurrentBal
from confluence_dbo.ConfluenceNumbers cf 
join confluence_dbo.users us on cf.customerid = us.customerid
join confluence_dbo.customers c on c.id = us.customerid
join confluence_dbo.Wallets w on us.id = w.userid
join confluence_dbo.TodayWalletTransactions tw on tw.walletid = w.id
where nuban in (select wemaaccount from custom.nuban)
group by trunc(dategenerated),cf.Nuban,concat(concat(firstname,'  '),LastName))

''')

print(nuban.head(6))

print(nuban.info())




