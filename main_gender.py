# -*- coding: utf-8 -*-
"""
Created on Sun May 10 08:49:19 2020

@author: philv
"""

import os
from covid_scraper import MetaCollector,GenderStats
import datetime
import sys
import pandas as pd
import time

if __name__ == "__main__":
    platforms = ['medrxiv','biorxiv','arxiv','osf','preprints_org',
                 'nber','F1000']
    osf_platforms = ['psyarxiv','earthrxiv','socarxiv']
    all_platforms = platforms + osf_platforms
    args = sys.argv
    nb_args = len(args)
    mode = args[1]
    
    if mode == 'init':             
        if len(args) < 3:
            raise Exception("A starting date has to be provided with the init arg.")
        else:
            start_date = datetime.datetime.strptime(args[2],'%Y-%m-%d')
            with open(os.path.join("data","start_date.txt"),'w') as f:
                f.write(args[2])
            min_date_med = datetime.datetime.strptime('2019-06-25','%Y-%m-%d')
            out_path = os.path.join("data","meta")
            
            if not os.path.exists(out_path):
                os.makedirs(out_path)
            else:
                while True:
                    resp = input("Intializing a new session will erease previous records. Do you want to continue(y/n)?")
                    if resp == "y":
                        break
                    elif resp == "n":
                        sys.exit()
                
            for platform in platforms:
                with open(os.path.join(out_path,platform+'_gender.csv'),'w') as f:
                    header = ["ID","date","first_gender","last_gender","all_gender",
                              "middle_gender","nb_authors","nb_genders_id","nb_women"]
                    f.write('|'.join(header)+'\n')
                if platform == "medrxiv" and start_date < min_date_med:
                    start_date_file = min_date_med
                else:
                    start_date_file = start_date
                with open(os.path.join(out_path,platform+'.csv'),'w') as f:
                    if platform in ["arxiv","osf","preprints_org"]:
                        items = ["",start_date_file.strftime("%Y-%m-%d"),"","",""]
                    elif platform == "nber":
                        items = ["",start_date_file.strftime("%Y-%m"),"",""]
                    elif platform in ['medrxiv','biorxiv']:
                        items = ["",start_date_file.strftime("%Y-%m-%d"),"",""]
                    elif platform == "F1000":
                        items = ["",start_date_file.strftime("%Y-%m-%d"),"all","",""]
                        
                    f.write('|'.join(items)+'\n')
        mode = 'all'

                
    
    while True:
        if mode == "meta" or mode == "all" or mode == "periodic":
            
            ''' This will collect metadata on all submissions that were last updated 
            at the starting date or later. This means that the initial submission
            might be earlier than the starting date.
            '''
            for platform in platforms:
                MetaCollector.collect_data(platform)      
                if platform == 'osf':
                    MetaCollector.split_platform(platform,osf_platforms)

            '''
            Tag the submissions as COVID related based on a regex match in the title
            and in the abstract.
            '''
            #keywords = ['covid','corona','sars-cov-2','ncov']
            regex_search = "(\\s|\\b)(ncov)([^a-z]|\\b)|(\\s|\\b)(corona)[\\s-]?(virus)([^a-z]|\\b)|(\\s|\\b)(sars-cov-2)([^a-z]|\\b)|(\\s|\\b)(covid)([^a-z]|\\b)"
            for platform in all_platforms:
                MetaCollector.tag_keywords_title(platform,regex_search)      
                
        if mode == "stats" or mode == "all" or mode == "periodic":
            print("Loading gender matcher.")
            gender = pd.read_csv(os.path.join('data','gender_data.txt'),sep='\t')
            gender['gender'] = gender['gender'].str.lower()
            gender_dict = {row[1]['First_Name']:row[1]['gender'] for row in gender.iterrows()}
            for platform in all_platforms:
                GenderStats.assign_gender(platform,gender_dict)
                
            GenderStats.combine_platforms(all_platforms)
            
        print("Data up to date!")
        if mode == "periodic":
            #Run again in 24 hours
            print("Sleeping for 24 hours.")
            time.sleep(60*60*24)
        else:
            break
   
