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

rates = read_sql_query_dwh('''select id, FromCurrencyId,ToCurrencyId,FromCurrencyName currency, ToCurrencyName, buy, sell, BaseRate,DateCreated 
        from confluence_dbo.NewRates where ToCurrencyId = 4 order by ToCurrencyId asc''')

# Connection to the DWH
print(rates)

