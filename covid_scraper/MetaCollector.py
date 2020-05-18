# -*- coding: utf-8 -*-
"""
Created on Sun May 10 08:55:58 2020

@author: philv
"""
import os
import csv
import datetime
from bs4 import BeautifulSoup as bs
import cfscrape
import time
import numpy as np

import logging
import requests
import xml.etree.cElementTree as ET
from nltk.tokenize import word_tokenize, sent_tokenize
import re
import pandas as pd

    
def find_last_day_collect(platform):
    dates = set()
    with open(os.path.join("data","meta",platform + '.csv'),'r',encoding='utf-8') as f:
        reader = csv.reader(f,delimiter='|')
        for line in reader:
            dates.add(line[1])
    dates = [datetime.datetime.strptime(x,'%Y-%m-%d') for x in dates]
    most_recent = max(dates)
    return most_recent

def collect_data(platform):
    start_date = find_last_day_collect(platform)
    if platform in ['medrxiv','biorxiv']:
        collect_MB(platform,start_date)
    elif platform == 'arxiv':
        collect_arxiv(start_date)
    elif platform == 'osf':
        collect_osf(start_date)
    elif platform == 'preprints_org':
        collect_preprints_org(start_date)
       
def date_range(date1, date2):
    dates = []
    for n in range(int ((date2 - date1).days)+1):
        dates.append(date1 + datetime.timedelta(n))
    return dates

#Function to collect data from bioRxiv and medRxiv
def collect_MB(platform,start_date):
    scraper = cfscrape.create_scraper() # returns a requests.Session object
    url_base = "http://{0}.org/search/jcode%3A{0}%20limit_from%3A{1}%20limit_to%3A{1}%20numresults%3A100%20format_result%3Astandard?page={2}"
    now = datetime.datetime.now()
    dates_to_collect = date_range(start_date,now)
    nb_dates = len(dates_to_collect)
    with open(os.path.join('data','meta',platform+'.csv'),'a',encoding='utf-8') as f:
        for i in range(nb_dates):
            dt = dates_to_collect[i].strftime("%Y-%m-%d")
            print('{}: collecting metadata {} , {}/{}.'.format(platform,dt,i+1,nb_dates))
            page=0
            while True:
                time.sleep(5) 
                url_search = url_base.format(platform,dt,page)
                attempts = 0
                max_attempts = 3
                while True:
                    try:
                        html = bs(scraper.get(url_search).text,"html.parser")
                        break
                    except:
                        if attempts == 3:
                            raise Exception('No response from host.')
                        print('Host not responding, attempt {}/{}.'.format(attempts,max_attempts))
                        time.sleep(60)
                        attempts += 1
                # Collect the articles in the result in a list
                articles = html.find_all('li', attrs={'class': 'search-result'})
                if len(articles) == 0:
                    break
                if page == 0:
                    temp = html.find('section',attrs={'id':'section-content'}).find('div', attrs={'class':"pane-content"}).text
                    temp = temp.strip()
                    nb_results = int(temp.split(' ')[0])
                    nb_pages = int(np.ceil(nb_results/100))
                
                for j in range(len(articles)):
                    article = articles[j]
                    try:
                        art_doi = article.find('span', attrs={'class': "highwire-cite-metadata-doi highwire-cite-metadata"})
                        art_doi = ':'.join(art_doi.text.split(':')[1:]).strip()
                        # Pull the title, if it's empty then skip it
                        title = article.find('span', attrs={'class': 'highwire-cite-title'})
                        if title is None:
                            continue
                        title = title.text.strip()
                        
                        
                        # Now collect author information
                        authors = article.find_all('span', attrs={'class': 'highwire-citation-author'})
                        all_authors = []
                        for author in authors:
                            name = author.text
                            name = name.split(' ')
                            name = '/'.join([name[-1]] + [' '.join(name[:-1])])
                            all_authors.append(name)
                        all_authors = ';'.join(all_authors)
                        items = [art_doi,dt,title,all_authors]
                        items = [x.replace('|',' ') for x in items]
                        items = [x.replace('\n',' ') for x in items]
                        f.write('|'.join(items)+'\n')  
                    except AttributeError:
                        continue
                if page == nb_pages - 1:
                    break
                else:
                    page += 1
 
def collect_osf(start_date):
    from selenium import webdriver
    urlpage = 'https://osf.io/preprints/discover?page={}&q=date%3A{}%20'
    driver = webdriver.Firefox(executable_path = os.path.join('tools','geckodriver.exe'))
    platform = 'osf'
    now = datetime.datetime.now()
    dates_to_collect = date_range(start_date,now)
    nb_dates = len(dates_to_collect)
    with open(os.path.join('data','meta',platform+'.csv'),'a',encoding='utf-8') as f:
        for i in range(nb_dates):
            dt = dates_to_collect[i].strftime("%Y-%m-%d")
            print('{}: collecting metadata {} , {}/{}.'.format('osf',dt,i+1,nb_dates))
            page = 1
            while True:
                # get web page
                driver.get(urlpage.format(page,dt))
                nb_try = 0
                while True:
                    time.sleep(5)
                    source = driver.execute_script("return document.getElementsByTagName('html')[0].innerHTML")
                    html = bs(source, 'html.parser')
                    articles = html.find_all('div', attrs={'class': "__search-result__3ede2 ember-view"})
                    nb_articles = len(articles)
                    if nb_articles == 0:
                        if nb_try == 1:
                            break
                        else:
                            nb_try += 1
                    else:
                        break
                if nb_articles == 0:
                    print('No articles found')
                    break
                for article in articles:
                    title = article.find_all('a')[-1]
                    link = title.attrs['href'].strip()
                    title = title.text.strip()
            
                    authors = article.find_all('li', attrs={'class': 'ember-view'})
                   
                    all_authors = []
                    for author in authors:
                        name = author.text
                        name = name.split(' ')
                        name = '/'.join([name[-1]] + [' '.join(name[:-1])])
                        all_authors.append(name)
                    all_authors = ';'.join(all_authors)
                    subjects = article.find_all('span', attrs={'class': 'subject-preview pointer'})
                    list_subjects = []
                    for subject in subjects:
                        list_subjects.append(subject.text.strip())
                    list_subjects = ';'.join(list_subjects)
                    platform = article.find('span', attrs={'class': 'search-result-providers'}).text.strip()
                    platform = platform.split('|')
                    platform = '/'.join([x.strip() for x in platform])

                    f.write('|'.join([link,dt,platform,title,all_authors]) + '\n')
                    
                page += 1 
                
def collect_preprints_org(start_date):        
    scraper = cfscrape.create_scraper() # returns a requests.Session object
    url_base = "https://www.preprints.org/search?search1=*&field1=title_keywords&clause=AND&search2=&field2=authors&search_subject_area=&search_subject_sub_area=&date_from={}&date_to={}&search_btn=&page_num={}"
    platform = 'preprints_org'
    now = datetime.datetime.now()
    dates_to_collect = date_range(start_date,now)
    dates_to_collect.append(datetime.datetime.now()+ datetime.timedelta(days=1))
    nb_dates = len(dates_to_collect)
    with open(os.path.join('data','meta',platform+'.csv'),'a',encoding='utf-8') as f:
        for i in range(nb_dates-1):
            dt1 = dates_to_collect[i].strftime("%Y-%m-%d")
            dt2 = dates_to_collect[i+1].strftime("%Y-%m-%d")
            print('{}: collecting metadata {} , {}/{}.'.format(platform,dt1,i+1,nb_dates))
            page=1
            while True:
                time.sleep(5)
                url_page = url_base.format(dt1,dt2,page)
                html = bs(scraper.get(url_page).text)
                # Collect the articles in the result in a list
                articles = html.find_all('div', attrs={'class': 'search-content-box margin-serach-wrapper-left'})
                if len(articles) == 0:
                    break
                for j in range(len(articles)):
                    article = articles[j]
                    title_data = article.find('a', attrs={'class': 'title'})
                    title = title_data.text
                    art_doi = title_data.attrs['href']
                        
                    # Now collect author information
                    authors = article.find_all('a', attrs={'class': 'author-selector'})
                    all_authors = []
                    for author in authors:
                        name = author.text
                        name = name.split(' ')
                        name = '/'.join([name[-1]] + [' '.join(name[:-1])])
                        all_authors.append(name)
                    all_authors = ';'.join(all_authors)
                                        
                    subjects = []
                    metadata = article.find_all('a')
                    for tag in metadata:
                        if tag.has_attr("href"):
                            ref = tag.attrs['href']
                            if "subject" in ref:
                                subjects.append(tag.text.strip())
                    subjects = ';'.join(subjects)
                    f.write('|'.join([art_doi,dt1,subjects,title,all_authors])+'\n')  

                page += 1

    
def download(url,start_date,resume_re,logging,record_tag,format_tag):
    max_tries=10
    params = {"verb": "ListRecords", "metadataPrefix": "arXiv", 
              "from": start_date}

    failures = 0
    while True:
        # Send the request.
        attempts = 0
        max_attempts = 3
        while True:
            try:
                r = requests.post(url, data=params)
                break
            except requests.exceptions.ConnectionError:
                if attempts == 3:
                    raise Exception('No response from host.')
                print('Host not responding, attempt {}/{}.'.format(attempts,max_attempts))
                time.sleep(60)
                attempts += 1
        code = r.status_code

        # Asked to retry
        if code == 503:
            to = int(r.headers["retry-after"])
            logging.info("Got 503. Retrying after {0:d} seconds.".format(to))

            time.sleep(to)
            failures += 1
            if failures >= max_tries:
                logging.warn("Failed too many times...")
                break

        elif code == 200:
            failures = 0

            # Write the response to a file.
            content = r.text
            yield parse(content,record_tag,format_tag)

            # Look for a resumption token.
            token = resume_re.search(content)
            if token is None:
                break
            token = token.groups()[0]

            # If there isn't one, we're all done.
            if token == "":
                logging.info("All done.")
                break

            logging.info("Resumption token: {0}.".format(token))

            # If there is a resumption token, rebuild the request.
            params = {"verb": "ListRecords", "resumptionToken": token}

            # Pause so as not to get banned.
            to = 10
            logging.info("Sleeping for {0:d} seconds so as not to get banned."
                         .format(to))
            time.sleep(to)

        else:
            r.raise_for_status()

def parse(xml_data,record_tag,format_tag):
    tree = ET.fromstring(xml_data)
    results = []
    for i, r in enumerate(tree.findall(record_tag)):
        try:
            arxiv_id = r.find(format_tag("id")).text
            date_art = r.find(format_tag("created")).text
            title = r.find(format_tag("title")).text
            categories = r.find(format_tag("categories")).text
            abstract = r.find(format_tag("abstract")).text.strip()
            abstract = abstract.replace('\n', ' ')
            authors = []
            for aut in r.find(format_tag("authors")).findall(format_tag("author")):
                keyname = aut.find(format_tag("keyname")).text
                forename = aut.find(format_tag("forenames")).text
                authors.append(keyname + '/' + forename)
            authors = ';'.join(authors)
        except:
            pass
        else:
            results.append((arxiv_id, date_art, title, authors, categories,abstract))
    return results

       
def collect_arxiv(start_date):           
    ''' The code for this scaper was adapted from
        https://github.com/dfm/data.arxiv.io
    '''
    start_date = start_date.strftime("%Y-%m-%d")
    logging.basicConfig(level=logging.INFO)
    # Download constants
    resume_re = re.compile(r".*<resumptionToken.*?>(.*?)</resumptionToken>.*")
    url = "http://export.arxiv.org/oai2"
    
    # Parse constant
    record_tag = ".//{http://www.openarchives.org/OAI/2.0/}record"
    format_tag = lambda t: ".//{http://arxiv.org/OAI/arXiv/}" + t   
    
    dl = download(url,start_date,resume_re,logging,record_tag,format_tag)
    for data in dl:
        for arxiv_id, date_art, title, authors, categories, abstract in data:
            c = categories.split()[0].split(".")[0].replace("/", "-")
            with open(os.path.join("data","meta","arxiv.csv"),
                           "a",encoding='utf-8') as f:
                items = [
                    arxiv_id,
                    date_art,
                    c,
                    " ".join(map(" ".join,
                                 map(word_tokenize,
                                     sent_tokenize(title)))),
                    " ".join(map(" ".join,
                                 map(word_tokenize,
                                     sent_tokenize(authors))))]
                items = [x.replace('|',' ') for x in items]
                items = [x.replace('\n',' ') for x in items]
                f.write("|".join(items) + "\n")
  
    
def tag_keywords_title(platform,regex_search):
    meta_data = pd.read_csv(os.path.join("data","meta",platform+".csv"),
                            sep="|",header=None,error_bad_lines=False)

    if platform in ['biorxiv','medrxiv']:
        meta_data.columns = ["ID","date","title","authors"]
    elif platform == 'arxiv':
        meta_data.columns = ["ID","date","sub","title","authors"]
    
    meta_data['title'] = meta_data['title'].str.lower()
    meta_data.loc[meta_data['title'].isnull(),"title"] = ''
    meta_data['key_related'] = meta_data['title'].apply(lambda x: re.search(regex_search,x) is not None)
    meta_data[['ID','key_related']].to_csv(os.path.join("data","meta",platform+"_key.csv"),index=False,sep='|')
