#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import requests
import bs4
from bs4 import BeautifulSoup
import pandas as pd
import time
import urllib  # Website connections
import re
import datetime as dt
import json
import os
from datetime import timedelta
from elasticsearch import Elasticsearch
import logging
import spacy



es = Elasticsearch([{'host': 'jre_elasticsearch_1'}])

# In[3]:


nlp = spacy.load('en_core_web_sm')

# In[4]:
skills_list = open('/root/airflow/dags/skills_set.csv').read().split('\n')


# In[5]:


def GTA_data_science_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="data+science"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DS_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DS_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="data+science"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DS_URLS.append(URL)
    return ALL_DS_URLS


def GTA_data_scientist_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="data+scientist"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DS_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DS_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="data+scientist"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DS_URLS.append(URL)
    return ALL_DS_URLS


# In[6]:


def GTA_data_engineer_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="data+engineer"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DE_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DE_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="data+engineer"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DE_URLS.append(URL)
    return ALL_DE_URLS


# In[7]:


def GTA_data_analyst_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="data+analyst"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DA_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DA_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="data+analyst"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DA_URLS.append(URL)
    return ALL_DA_URLS


def GTA_data_analysis_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="data+analysis"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DA_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DA_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="data+analysis"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DA_URLS.append(URL)
    return ALL_DA_URLS


def GTA_business_intelligence_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="business+intelligence"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DA_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DA_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="business+intelligence"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DA_URLS.append(URL)
    return ALL_DA_URLS


# In[8]:


def GTA_business_analyst_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="business+analyst"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DA_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DA_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="business+analyst"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DA_URLS.append(URL)
    return ALL_DA_URLS


def GTA_software_engineer_jobs_URLS():
    add = 0
    URL = 'https://www.indeed.ca/jobs?q="software+engineer"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
    page = requests.get(URL)
    soup = BeautifulSoup(page.text, 'html.parser')
    ALL_DA_URLS = []
    while soup.findAll('span', attrs={'class': 'pn'})[-1].text[:1] == 'N':
        ALL_DA_URLS.append(URL)
        add += 20
        URL = 'https://www.indeed.ca/jobs?q="software+engineer"&l=Toronto%2C+ON&radius=100&start={}'.format(add)
        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
    else:
        ALL_DA_URLS.append(URL)
    return ALL_DA_URLS


def fetch_total_pages_urls():
    All_URLS = GTA_data_science_jobs_URLS() + GTA_data_scientist_jobs_URLS() + GTA_data_engineer_jobs_URLS() + \
               GTA_data_analyst_jobs_URLS() + GTA_data_analysis_jobs_URLS() + GTA_business_intelligence_jobs_URLS() + \
               GTA_business_analyst_jobs_URLS() + GTA_software_engineer_jobs_URLS()

    return All_URLS


###run


# In[9]:


All_URLS = fetch_total_pages_urls()


# In[10]:


def get_jobs_ids(URLS):
    job_links = []
    job_ids = []

    for URL in URLS:

        page = requests.get(URL)
        soup = BeautifulSoup(page.text, 'html.parser')
        job_tags = soup.findAll('div', attrs={'class': 'title'})
        for i in range(len(job_tags)):
            if job_tags[i].a['id'][:2] != 'sj':
                job_links.append('http://indeed.ca' + job_tags[i].a['href'])
        for link in job_links:
            job_ids.append(re.findall(r'\w+[^a-z]fccid', link)[0][:16])
        job_tags = []
        job_links = []

    return job_ids


####' not run'


# In[11]:


def search_database_job_id(URLS):
    new_ids = []
    infile_online_ids = []
    infile_ids = []
    dropped_ids = []
    res = es.search(index="indeed_job_pool", body={"query": {"match_all": {}}}, size=10000, from_=0)
    IDS = set(get_jobs_ids(URLS))
    if len(res) == 0:
        for element in IDS:
            new_ids.append(element)

        return new_ids
    else:
        for num in range(len(res['hits']['hits'])):
            infile_ids.append(res['hits']['hits'][num]['_id'])

        for element in IDS:
            if element not in infile_ids:
                new_ids.append(element)
            else:
                infile_online_ids.append(element)

        for element in infile_ids:
            if element not in infile_online_ids:
                dropped_ids.append(element)

        return new_ids, dropped_ids


# In[12]:


id_infos = search_database_job_id(All_URLS)


# In[13]:


def extract_skills(resume_text):
    nlp_text = nlp(resume_text)
    noun_chunks = nlp_text.noun_chunks
    # removing stop words and implementing word tokenization
    tokens = [token.text for token in nlp_text if not token.is_stop]

    # extract values
    skills_list = open('/root/airflow/dags/skills_set.csv').read().split('\n')

    skillset = []

    # check for one-grams (example: python)
    for token in tokens:
        if token.lower() in skills_list:
            skillset.append(token)

    # check for bi-grams and tri-grams (example: machine learning)
    for token in noun_chunks:
        token = token.text.lower().strip()
        if token in skills_list:
            skillset.append(token)

    return [i.capitalize() for i in set([i.lower() for i in skillset])]


# In[14]:


def fetch_jobs_into_elasticsearch(jobs_ids):
    es = Elasticsearch([{'host': 'jre_elasticsearch_1'}])


    job_num = 1
    if len(jobs_ids) > 2:
        new_ids = jobs_ids
    else:
        new_ids = jobs_ids[0]
        dropped_ids = jobs_ids[1]
    for job_id in new_ids:
        try:
            job_url = 'https://ca.indeed.com/viewjob?jk={}'.format(job_id)
            page = requests.get(job_url)
            soup = BeautifulSoup(page.text, 'html.parser')
            job_description = \
                soup.findAll('div', attrs={'class': 'jobsearch-JobComponent-description icl-u-xs-mt--md'})[0].text
            job_title = \
                soup.findAll('h3', attrs={'class': 'icl-u-xs-mb--xs icl-u-xs-mt--none jobsearch-JobInfoHeader-title'})[
                    0].text
            job_company = soup.findAll('div', attrs={'class': 'icl-u-lg-mr--sm icl-u-xs-mr--xs'})[0].text
            job_location = \
                          soup.findAll('span', attrs={'class': 'jobsearch-JobMetadataHeader-iconLabel'})[0].text
            job_posted_time = re.findall(r'\d+ \w', re.sub(r'\W+', ' ', soup.findAll('div', attrs={
                'class': 'jobsearch-JobMetadataFooter'})[0].text))[0]
            try:
                if job_posted_time[-1] == 'd':
                    job_posted_date = (dt.datetime.now() - timedelta(int(job_posted_time.split()[0]))).strftime(
                        '%Y-%m-%d')
                elif job_posted_time[-1] == 'h':
                    job_posted_date = str(dt.datetime.now()).split()[0]
                elif job_posted_time[-1] == 'm':
                    job_posted_date = (dt.datetime.now() - timedelta(int(job_posted_time.split()[0]) * 30)).strftime(
                        '%Y-%m-%d')

            except:
                job_posted_date = (dt.datetime.now() - timedelta(90)).strftime('%Y-%m-%d')

            doc = {
                'job_id': job_id,
                'job_posted_date': job_posted_date,
                'job_title': job_title,
                'job_company': job_company,
                'location': job_location,
                'job_url': job_url,
                'job_description': job_description,
                'key_skills': extract_skills(job_description)
            }
            res = es.index(index="indeed_job_pool", doc_type='dictionary', id=job_id, body=doc)
            print(res['result'])

            res = es.get(index="indeed_job_pool", doc_type='dictionary', id=job_id)
            print(res['_source'])

            es.indices.refresh(index="indeed_job_pool")
        except:
            continue

    for job_id in dropped_ids:

        try:
            res = es.delete(index="indeed_job_pool", doc_type='dictionary', id=job_id)
            print(res['result'] + ' {}'.format(job_id))
        except:
            continue


if __name__ == "__main__":
    fetch_jobs_into_elasticsearch(jobs_ids=id_infos)

# In[ ]:

