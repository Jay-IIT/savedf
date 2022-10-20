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
        self.__progress_df__ = self.__load_progess_df__()
      
    def __load_progess_df__(self):
        if not (os.path.exists(self.__file__)):
            df = self.__pruned_df__
            df['progress'] = self.__pruned_df__['stats']
        else:
            with open(self._runs, 'rb') as f:
                self.__runlist__ = pickle.load(f) 
            df = pd.read_pickle(self.__file__)
            df = df.drop('progress')
            df = df.rename({'stats': f'stats{self.__runlist__[-1]}'}, axis='columns')
            df = self.__compute_df__(df)
        self.__pickle_df__()
        return df

    def __compute_df__(self,df):
        if len(df.columns) > 6:
            df = df.drop(f'stats{self.__runlist__[0]}')
            self.__runlist__.pop(0)
        join_df = pd.merge(df[['module','owner','coverage']],self.__pruned_df__[['module','owner','coverage']],
                          left_on=['module','owner'],
                          right_on=['module','owner'],how='outer',suffixes=['_df_1','_df_2'])
        df['progress'] = join_df.apply(lambda row : difference(row['coverage_df_1'],
                     row['coverage_df_2']), axis = 1)
        def difference(stats_prev,stats_curr):
            if stats_prev is np.nan or stats_curr is np.nan:
                return " "
            stats_cur =  {list(filter(None, re.split(r'(\d+)', k)))[1]:list(filter(None, re.split(r'(\d+)', k)))[0] for k in stats_curr.split("/")}
            stats_prev = {list(filter(None, re.split(r'(\d+)', k)))[1]:list(filter(None, re.split(r'(\d+)', k)))[0] for k in stats_prev.split("/")}
            res = ""
            for k,v in stats_curr:
                if k in stats_prev:
                    res += f"{v-stats_prev[k]}/"
            return res[:-1]
        return df


    
    def __pickle_df__(self):
        pd.to_pickle(self.__progress_df__,self.__file__)
        self.__runlist__.append(datetime.today().strftime('%m-%d-%y'))
        with open(self.__runs__ , 'wb') as f:
            pickle.dump(self.__runlist__, f)  

  
 




Progress()
 