from pandas.io.json import json_normalize
import json, requests
from datetime import datetime
import sqlalchemy as sa

def rel_to_proper(dt):
    mydt = dateparser.parse(dt)
    return mydt

u='https://services1.arcgis.com/0MSEUqKaxRlEPj5g/arcgis/rest/services/ncov_cases/FeatureServer/1/query?f=json&where=1=1&returnGeometry=false&spatialRel=esriSpatialRelIntersects&outFields=*&orderByFields=Confirmed%20desc&outSR=102100&resultOffset=0&resultRecordCount=160&cacheHint=true'

r=requests.get(u)
e=sa.create_engine('mysql://dsn')
x=json.loads(r.content)
#print(x)
dd=json_normalize(x['features'])
d=dd[['attributes.Province_State', 'attributes.Country_Region', 'attributes.Last_Update',
    'attributes.Lat', 'attributes.Long_', 'attributes.Confirmed', 'attributes.Deaths','attributes.Recovered' ]]
d.columns=['state', 'country', 'last_update', 'lat', 'lng', 'confirmed', 'deaths', 'recovered'  ]
d['posted_date']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')  #pd.to_datetime(d['posted_date'])
print (d.head())
d.to_sql('arcgis' , e, if_exists='append', index = False, )


