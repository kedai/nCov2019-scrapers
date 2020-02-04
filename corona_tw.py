#import pandas as pd
from birdy.twitter import UserClient
from pandas.io.json import json_normalize
import sqlalchemy as sa

ckey='your jkey'
csecret='your secret'
token='token'
token_secret='secret'

c=UserClient(ckey, csecret, token, token_secret)

r=c.api.statuses.user_timeline
resp=r.get(screen_name='mugecevik', count=100)

df=json_normalize(resp.data)
df.to_sql('mugecevik_tw' , e, if_exists='append', index = False, )

