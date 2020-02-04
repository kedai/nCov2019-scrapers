from pandas.io.json import json_normalize
import json
import dateparser
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
import sqlalchemy as sa

def bmdt_to_dt(x):
    hari={'Jumaat':'Friday', 'Isnin':'Monday', 'Selasa':'Tuesday', 'Rabu':'Wednesday','Khamis':'Thursday',
     'Sabtu':'Saturday', 'Ahad':'Sunday'}
    bulan={'Januari':'January', 'Februari':'February','Mac':'March', 'April':'April', "Mei":'May',
           'Jun':'June','Julai':'July','Ogos':'August', 'September':'September','Oktober':'October',
        'November':'November','Disember':'December'
    }
    h,dt=x.split(',')
    newh=hari[h]
    dt_str=dt.split()
    dd=dt_str[0]
    mm=bulan[dt_str[1]]
    YY=dt_str[2]
    HHMM=dt_str[-1]
    conv_dt='{} {} {} {}'.format(dd, mm, YY, HHMM)
    return conv_dt


def prepare_dict(o):
    d={}
    d['title']=o.find_all("a")[-1].getText()
    d['url']='https://bharian.com.my'+ o.find_all("a")[-1]['href']
    d['description']=o.find("p").getText()
    d['content']=o.find("p").getText()
    d['urlToImage']=o.find_all("img")[0]['data-src']  #'https://bharian.com.my'+o.find_all("a")[0]['href']
    dt_str=bmdt_to_dt(o.find_all("span", class_="field-content")[-2].getText() )
    d['publishedAt']=rel_to_proper(dt_str).strftime('%Y-%m-%d %H:%M:%S')
    d['addedOn']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    d['siteName']='BH'
    d['language']='ms'
    return d

def rel_to_proper(dt):
    mydt = dateparser.parse(dt)
    return mydt

if __name__ == '__main__':
    headers={'User-Agent':'Mozilla/5.0 (Linux; Android 4.0.4; Galaxy Nexus Build/IMM76B) AppleWebKit/535.19 (KHTML, like Gecko) Chrome/18.0.1025.133 Mobile Safari/535.19'}
    r=requests.get('https://www.bharian.com.my/search?s=koronavirus', headers=headers)
    l=[]
    s=bs(r.content, features="lxml")
    x=s.find_all('div', class_='views-row')
    for i in x:
        d=prepare_dict(i)
        l.append(d)
    df=json_normalize(l)
    e=sa.create_engine('mysql://dsn')
    df.to_sql('newsapi_n_bernama_temp' , e, if_exists='append', index = False, )
    with e.begin() as cnx:
        insert_sql = 'INSERT IGNORE INTO newsapi_n (SELECT * FROM newsapi_n_bernama_temp)'
        cnx.execute(insert_sql)

