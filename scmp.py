from pandas.io.json import json_normalize
import json
import dateparser
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
import sqlalchemy as sa
import html2text


def prepare_dict(o):
    d={}
    d['title']=o.find_all("a")[-1].getText()
    d['url']='https://hmetro.com.my'+ o.find_all("a")[-1]['href']
    d['description']=o.find("p").getText()
    d['content']=o.find("p").getText()
    d['urlToImage']=o.find_all("img")[0]['src']  #'https://bharian.com.my'+o.find_all("a")[0]['href']
    dt_str=bmdt_to_dt(o.find_all("span", class_="field-content")[-2].getText() )
    d['publishedAt']=rel_to_proper(dt_str).strftime('%Y-%m-%d %H:%M:%S')
    d['addedOn']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    d['siteName']='HM'
    d['language']='ms'
    return d

def rel_to_proper(dt):
    mydt = dateparser.parse(dt)
    return mydt

if __name__ == '__main__':
    headers={'authority': 'apigw.scmp.com', 'apikey': 'yourkey',
           'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/79.0.3945.130 Safari/537.36',
         'origin': 'https://www.scmp.com','sec-fetch-site': 'same-site', 'sec-fetch-mode': 'cors',
         'referer': 'https://www.scmp.com/search/coronavirus'
     }
    r=requests.get('https://apigw.scmp.com/search/v1?q=coronavirus+more:pagemap:metatags-cse_type:Article&offset=1&limit=10', headers=headers )

    dd=json.loads(r.content)['items']
    l=[]
    for i in dd:
        if 'publishdate' in i.keys():
            d={}
            d['title']=i['title']
            d['url']=i['url']
            d['description']=' '.join(html2text.html2text(i['snippet']).split('**')[2:]).replace('\n',' ')
            d['content']=d['description']
            d['urlToImage']=i['squareImage']
            d['publishedAt']=i['publishdate']
            d['addedOn']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            d['siteName']='SCMP'
            d['language']='en'
        l.append(d)
    df=json_normalize(l)
    e=sa.create_engine('mysql://dsn')
    df.to_sql('newsapi_n_bernama_temp' , e, if_exists='append', index = False, )
    with e.begin() as cnx:
        insert_sql = 'INSERT IGNORE INTO newsapi_n (SELECT * FROM newsapi_n_bernama_temp)'
        cnx.execute(insert_sql)

