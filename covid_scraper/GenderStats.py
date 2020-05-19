# -*- coding: utf-8 -*-
"""
Created on Fri May 15 16:26:16 2020

@author: philv
"""


import pandas as pd
import os
import numpy as np
import datetime

def assign_gender(platform,gender_dict):
    meta_data = pd.read_csv(os.path.join("data","meta",platform+'.csv'),encoding='utf-8',header=None,
                            sep="|",error_bad_lines=False)
    if platform in ['arxiv','F1000','psyarxiv','socarxiv','eartharxiv','preprints_org']:
        meta_data.columns = ["ID","date","sub","title","authors"]
    else:
        meta_data.columns = ["ID","date","title","authors"]
        
    gender_data = pd.read_csv(os.path.join("data","meta",platform+'_gender.csv'),
                              sep="|")
    
    analyzed_submissions = gender_data["ID"]
    meta_data = meta_data.loc[~meta_data["ID"].isin(analyzed_submissions)]
    
  
    meta_data['date_str'] = meta_data['date'].copy()
    if platform == "nber":
        meta_data['date'] = pd.to_datetime(meta_data['date'],format='%Y-%m',errors='coerce')
    else:
        meta_data['date'] = pd.to_datetime(meta_data['date'],format='%Y-%m-%d',errors='coerce')
    meta_data = meta_data.sort_values('date').drop_duplicates(['title','ID','authors'],keep='first')
   
    nb_rows = len(meta_data)
    meta_data.loc[meta_data['authors'].isnull(),'authors'] = ''
    with open(os.path.join("data","meta",platform+'_gender.csv'),'a') as f:
        for row_i in range(nb_rows):
            row = meta_data.iloc[row_i]
            ID = row['ID']
            authors = row['authors']
            if len(authors) == 0:
                continue
            authors = authors.lower()
            list_authors = authors.split(';')
            genders = []
            nb_authors = len(list_authors)
            
            for j in range(nb_authors):
                author = list_authors[j]
                try:
                    first = author.split('/')[1]
                    first = first.split(' ')[0].strip()
                except IndexError:
                    first = np.nan
                if first in gender_dict:
                    gen_auth = gender_dict[first]
                else:
                    gen_auth = None
                genders.append(gen_auth)
                
            genders_num = []
            nb_matched = 0
            for gen in genders:
                if gen == 'm':
                    genders_num.append(0)
                    nb_matched += 1
                elif gen == 'f':
                    genders_num.append(1)
                    nb_matched += 1
                else:
                    genders_num.append(np.nan)  
            if nb_authors > 2:
                mid_gender = np.nanmean(genders_num[1:-1])
            else:
                mid_gender = np.nan
            items = [ID,row['date_str'],genders_num[0],genders_num[-1],
                     np.nanmean(genders_num),mid_gender,
                     len(list_authors),nb_matched,
                     np.nansum(genders_num)]
            items = map(str,items)
            f.write('|'.join(items)+'\n')
           
def combine_platforms(platforms):
    df_list = []
    for platform in platforms:
        df = pd.read_csv(os.path.join("data","meta",platform+'_gender.csv'),
                         sep="|")
        df['platform'] = platform
        df['type'] = "preprints"
        df_list.append(df)
        
    with open(os.path.join("data","start_date.txt"),'r') as f:
        start_date = f.read()
    now = datetime.datetime.now().strftime("%Y-%m-%d")
    all_df = pd.concat(df_list, axis=0, ignore_index=True)
    all_df['date'] = pd.to_datetime(all_df['date'],format='%Y-%m-%d',errors='coerce')
    all_df = all_df[all_df['date'].between(start_date, now)]
    
    all_df.to_csv(os.path.join("data","all_data.csv"), mode='w',sep="|",
                  index=False)
    
