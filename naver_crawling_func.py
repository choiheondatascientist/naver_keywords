#!/usr/bin/env python
# coding: utf-8

# In[1]:


import requests
import json
import pymongo
from bs4 import BeautifulSoup
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# In[ ]:


# pip install mysqlclient


# In[ ]:


# sudo apt-get install mysql-serve
# sudo apt-get install libmysqlclient-dev
# pip install mysqlclient
# pip install sqlalchemy
# pip install pymongo
# pip install bs4


# #### Mysql

# In[2]:


mysql_client = create_engine("mysql://root:dssf@52.78.143.83/terraform?charset=utf8")


# In[3]:


# __tablename__ : 데이터 베이스 테이블 이름
base = declarative_base()

class NaverKeywords(base):
    __tablename__ = "naver"

    id = Column(Integer, primary_key=True)
    rank = Column(Integer, nullable=False)
    keyword = Column(String(50), nullable=False)
    rdate = Column(TIMESTAMP, nullable=False)

    def __init__(self, rank, keyword):
        self.rank = rank
        self.keyword = keyword

    def __repr__(self):
        return "<NaverKeywords {}, {}>".format(self.rank, self.keyword)


# In[4]:


def crawling():
    response = requests.get("https://www.naver.com/")
    dom = BeautifulSoup(response.content, "html.parser")
    keywords = dom.select(".ah_roll_area > .ah_l > .ah_item")
    datas = []
    for keyword in keywords:
        rank = keyword.select_one(".ah_r").text
        keyword = keyword.select_one(".ah_k").text
        datas.append((rank, keyword))
    return datas


# In[5]:


def mysql_save(datas, mysql_client=mysql_client):
    
    keywords = [NaverKeywords(rank, keyword) for rank, keyword in datas]
    
    # make session
    maker = sessionmaker(bind=mysql_client)
    session = maker()

    # save datas
    session.add_all(keywords)
    session.commit()

    # close session
    session.close()


# #### Mongodb

# In[6]:


mongo_client = pymongo.MongoClient('mongodb://52.78.143.83:27017')


# In[7]:


def mongo_save(datas, mongo_client=mongo_client):
    querys = [{"rank":rank, "keyword":keyword} for rank, keyword in datas]
    mongo_client.terraform.naver_keywords.insert(querys)


# #### Send Slack

# In[8]:


def send_slack(msg, channel="#program", username="provision_bot" ):
    webhook_URL = "https://hooks.slack.com/services/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"
    payload = {
        "channel": channel,
        "username": username,
        "icon_emoji": ":provision:",
        "text": msg,
    }
    response = requests.post(
        webhook_URL,
        data = json.dumps(payload),
    )
    return response


# #### Run Function

# In[9]:


def run():
    
    # 데이터 베이스에 테이블 생성
    base.metadata.create_all(mysql_client)

    # 네이버 키워드 크롤링
    datas = crawling()

    # 데이터 베이스에 저장
    mysql_save(datas)
    mongo_save(datas)

    # 슬랙으로 메시지 전송
    send_slack("naver crawling done!")


# In[10]:


run()


# In[ ]:




