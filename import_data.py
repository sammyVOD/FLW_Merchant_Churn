import sqlalchemy as sa
import pandas as pd
engine = sa.create_engine(
    '**************************************************************'
)
data_frame = pd.read_sql(
    'select * from merchant_churn.collections;', engine
)

print(data_frame.head(5))
