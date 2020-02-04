from pandas.io.json import json_normalize
import json
import dateparser
import requests
from bs4 import BeautifulSoup as bs
from datetime import datetime
import sqlalchemy as sa


def prepare_dict(xx):
    d={}
    #print (xx)
    t=xx.find("a")
    img=xx.find("img")
    description=xx.find("div", class_="w3-x-padding")
    #print(t, img, description)
    try:
        d['title']=t.getText()
        d['description']=description.getText()
        d['author']=''
        d['url']='https://bernama.com/en/'+t['href']
        d['content']=description.getText()
        d['urlToImage']=img['src']
        d['publishedAt']=rel_to_proper(description.getText().split('--')[0].strip().split(',')[1].strip()+' 2020').strftime('%Y-%m-%d %H:%M:%S')
        d['addedOn']=datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        d['siteName']='Bernama'
        #d['language']='en'
    #except (TypeError, AttributeError) as e:
    except:
        pass
    return d

def rel_to_proper(dt):
    mydt = dateparser.parse(dt)
    return mydt

if __name__ == '__main__':
    languages=['http://bernama.com/en/search.php?cat=all&terms=coronavirus&submit=Search', 
           'http://bernama.com/bm/search.php?cat=all&terms=koronavirus&submit=Carian']
    l=[]
    for lng in languages:
        r=requests.get(lng)
        s=bs(r.content, features="lxml")
        x=s.find_all("div", class_="w3-justify")
        for i in x:
        #print (i)
            d=prepare_dict(i)
            if lng.find('com/en/')>1:
                d['language']='en'
            else:
                d['language']='ms'
            d['status']=1
            if d and len(d.keys())>=10:
                l.append(d)
    df=json_normalize(l)
    e=sa.create_engine('mysql://dsn')
    df.to_sql('newsapi_n_bernama_temp' , e, if_exists='append', index = False, )
    with e.begin() as cnx:
        insert_sql = 'INSERT IGNORE INTO newsapi_n (SELECT * FROM newsapi_n_bernama_temp)'
        cnx.execute(insert_sql)

