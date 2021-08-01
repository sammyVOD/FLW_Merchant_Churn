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

import pandas as pd
import numpy as np
import pathlib
import redshift_connector
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

print('Rayy')

