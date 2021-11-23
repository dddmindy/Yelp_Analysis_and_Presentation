#!/usr/bin/env python
# coding: utf-8

#
import pandas as pd
import seaborn as sns
import numpy as np
import matplotlib.pyplot as plt


tmdb = pd.read_csv("movies.csv")

tmdb.head()

tmdb.shape

tmdb.dtypes

tmdb.isna().sum()

# Data cleaning

small_tmdb = tmdb.loc[:,["popularity","original_title","director",'runtime',"genres"                       ,"vote_count","vote_average","budget_adj","revenue_adj"]]

small_tmdb.head()

small_tmdb.isna().sum()

small_tmdb[small_tmdb.director.isna()]

sns.distplot(small_tmdb['revenue_adj'].copy())

small_tmdb['revenue_adj'] = small_tmdb['revenue_adj'].replace({0:np.nan})
small_tmdb.shape

small_tmdb.dropna(inplace = True)

small_tmdb.isna().sum()
small_tmdb['log_rev_adj'] = np.log10(small_tmdb['revenue_adj'])


small_tmdb['budget_adj'] = small_tmdb['budget_adj'].replace(0, np.nan)
small_tmdb['log_bug_adj'] = np.log10(small_tmdb['budget_adj'])

small_tmdb.isna().sum()

revenue_adj_log = np.log10(small_tmdb.loc[:,"revenue_adj"])

sns.distplot(revenue_adj_log)

budget_adj_log = np.log10(small_tmdb.loc[small_tmdb.budget_adj>0,"budget_adj"].copy())

sns.distplot(budget_adj_log)

small_tmdb['profits_adj'] = small_tmdb['revenue_adj'] - small_tmdb['budget_adj']

rofits_adj = small_tmdb.loc[small_tmdb.profits_adj.notna(),'profits_adj'].copy()

sns.distplot(profits_adj )

sns.distplot(np.log10(small_tmdb['vote_count']))

sns.distplot(small_tmdb['vote_average'])


sns.scatterplot(x = "popularity", y = "log_rev_adj",data = small_tmdb,alpha = 0.3)


sns.jointplot(x = "popularity", y = "log_rev_adj",data = small_tmdb,kind="hex")

sns.scatterplot(x = "log_bug_adj", y = "log_rev_adj",data = small_tmdb)

small_tmdb['profit_adj'] = small_tmdb['revenue_adj'] - small_tmdb['budget_adj']

small_tmdb.groupby('director').agg({"profit_adj":"sum","director":"count"}).sort_values(by = "profit_adj",ascending = False).head()

small_tmdb["major_genre"] = small_tmdb['genres'].str.split("|",expand = True).iloc[:,0]    

plt.figure(figsize = (8,8))
sns.boxplot(x = "major_genre", y = "log_rev_adj", data = small_tmdb)
plt.xticks(rotation=60);
