#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

import json
import warnings
warnings.filterwarnings('ignore')
plt.rcParams['font.sans-serif'] = ['SimHei']
movies = pd.read_csv('movies.txt')


movies.shape
credits.shape
movies.columns
credits.columns
movies.describe()
credits.describe()
movies.info()
credits.info()
movies.head()
credits.head()


credits.drop('title', axis = 1, inplace = True)

full = pd.merge(movies, credits, how = 'left', left_on='id', right_on='movie_id')

full.info()


full.drop(['homepage', 'original_title', 'overview', 'spoken_languages', 'status', 'tagline', 'movie_id'], axis = 1, inplace = True)


full.loc[full['runtime'].isnull()]
full.loc[2656, 'runtime'] = 94
full.loc[4140, 'runtime'] = 240


movies[movies['release_date'].isnull()]
movies.loc[4553, 'release_date'] = '2010-06-01'


Cols = ['genres', 'keywords', 'production_companies', 'production_countries', 'cast', 'crew']

for col in Cols:
    full[col] = full[col].apply(json.loads)   
def getname(x):
    list = [i['name'] for i in x]
    return ','.join(list)

for col in Cols[0:4]:
    full[col] = full[col].apply(getname)
    
def getcharacter(x):
    list = [i['character'] for i in x]
    return ','.join(list[0:2])
    
full['cast'] = full['cast'].apply(getcharacter)

def getdirector(x):
    list = [i['name'] for i in x if i['job'] == 'Director']
    return ','.join(list)
    
full['crew'] = full['crew'].apply(getdirector)


genreset = set()
for x in full['genres'].str.split(','):
    genreset.update(x)
genreset.discard('')
genrelist = list(genreset)


genre_df = pd.DataFrame()

for genre in genrelist:
    genre_df[genre] = full['genres'].str.contains(genre).map(lambda x: 1 if x else 0)

full['release_date'] = pd.to_datetime(full['release_date'], format = '%Y-%m-%d').dt.year

name_dict = {'release_date': 'year', 'cast': 'actor', 'crew': 'director'} 
full.rename(columns = name_dict, inplace = True)



# Q5
full[['budget', 'popularity', 'revenue', 'runtime', 'vote_average', 'vote_count']].corr()
full[['budget', 'popularity', 'revenue', 'runtime', 'vote_average', 'vote_count']].corr().iloc[2]
revenue_df = full[['popularity', 'vote_count', 'budget', 'revenue']]

fig = plt.figure(figsize = (15, 5))

ax1 = plt.subplot(1, 3, 1)
ax1 = sns.regplot(x='popularity', y='revenue', data = revenue_df)  
ax1.text(400, 3e9, 'r=0.64', fontsize=12)
plt.xlabel('popularity', fontsize=12)
plt.ylabel('revenue', fontsize=12)
plt.title('revenue by popularity', fontsize=15)

ax2 = plt.subplot(1, 3, 2)
ax2 = sns.regplot(x='vote_count', y='revenue', data = revenue_df, color='g')  
ax2.text(5800, 2.1e9, 'r=0.78', fontsize=12)
plt.xlabel('vote_count', fontsize=12)
plt.ylabel('revenue', fontsize=12)
plt.title('revenue by rating_count', fontsize=15)

ax3 = plt.subplot(1, 3, 3)
ax3 = sns.regplot(x='budget', y='revenue', data = revenue_df, color='r')  
ax3.text(1.6e8, 2.1e9, 'r=0.73', fontsize=12)
plt.xlabel('budget', fontsize=12)
plt.ylabel('revenue', fontsize=12)
plt.title('revenue by budget', fontsize=15)

fig.savefig('revenue.png')



