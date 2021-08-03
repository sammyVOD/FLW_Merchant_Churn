import sqlalchemy as sa
import pandas as pd
engine = sa.create_engine(
    'redshift://flw_warehouse:t2vgvRd2wOh@flwdatawarehouse.cl7qyipyw37a.eu-west-2.redshift.amazonaws.com:5439/data_warehouse'
)
data_frame = pd.read_sql(
    'select * from merchant_churn.collections;', engine
)

print(data_frame.head(5))
