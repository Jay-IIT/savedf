import pandas as pd
import os 
import pickle 
import glob
from datetime import datetime
import numpy as np 
import re 

class Progress:
    def __init__(self,purned_df):
        self.__file__ = ".meta/progressdf.pkl"
        self.__runs__ =  ".meta/rundates.pkl"
        self.__pruned_df__ = purned_df
        self.__load_progess_df__()

    def get(self):
        return self.__progress_df__

    def __load_progess_df__(self):
        if not (os.path.exists(self.__file__)):
            try:
                os.makedirs(".meta")
            except Exception as e:
                pass
            df = self.__pruned_df__
            df['progress'] = self.__pruned_df__['Stats']
            self.__runlist__ = []
        else:
            with open(self.__runs__, 'rb') as f:
                self.__runlist__ = pickle.load(f) 
            df = pd.read_pickle(self.__file__)
            df = df.drop('progress',axis='columns')
            try:
                df = df.rename({'Stats': f'Stats{self.__runlist__[-1]}'}, axis='columns')
            except Exception as e:
                pass
            df['Stats'] = self.__pruned_df__['Stats']
            df = self.__compute_df__(df)
        self.__progress_df__ = df
        self.__pickle_df__()
        

    def __compute_df__(self,df):
        def difference(stats_prev,stats_curr):
           if stats_prev is np.nan or stats_curr is np.nan:
              return " "

           if "/" in stats_curr:
              stats_curr =  {list(filter(None, re.split(r'(\d+)', k)))[1]:list(filter(None, re.split(r'(\d+)', k)))[0] for k in stats_curr.split("/")}
           else:
              stats = list(filter(None, re.split(r'(\d+)', stats_curr)))
              stats_curr = {stats[1]:stats[0]}
           if "/" in stats_prev:
              stats_prev = {list(filter(None, re.split(r'(\d+)', k)))[1]:list(filter(None, re.split(r'(\d+)', k)))[0] for k in stats_prev.split("/")}
           else:
              stats = list(filter(None, re.split(r'(\d+)', stats_prev)))
              stats_prev = {stats[1]:stats[0]}
         

           res = ""
           for k,v in stats_curr.items():
               if k in stats_prev:
                   res += f"{int(v)-int(stats_prev[k])}{k}/"
           return res[:-1]
        
        if len(df.columns) > 6:
            df = df.drop(f'Stats{self.__runlist__[0]}',axis='columns')
            self.__runlist__.pop(0) 
        join_df = pd.merge(df[['module','owner',f'Stats{self.__runlist__[-1]}']],self.__pruned_df__[['module','owner','Stats']],
                          left_on=['module','owner'],
                          right_on=['module','owner'],how='outer',suffixes=['_df_1','_df_2'])
        df['progress'] = join_df.apply(lambda row : difference(row[f'Stats{self.__runlist__[-1]}'],row['Stats']), axis = 1)

       
        return df
    
    def __pickle_df__(self):
        pd.to_pickle(self.__progress_df__,self.__file__)
        self.__runlist__.append(datetime.today().strftime('%m-%d-%y'))
        with open(self.__runs__ , 'wb') as f:
            pickle.dump(self.__runlist__, f)




 