#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import ProcessData as p
import d-data-process as p
import matplotlib.pyplot as plt
from sklearn.linear_model import LinearRegression, LogisticRegression
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score, confusion_matrix

from sklearn.metrics import f1_score, make_scorer
from sklearn.model_selection import GridSearchCV

business_file = 'yelp_academic_dataset_business2.json'
business_df = pd.read_json(business_file, lines=True)


business_df.corr().style.format("{:.2}").background_gradient(cmap=plt.get_cmap('coolwarm'), axis=1business_df.groupby(by=['is_open']).mean()

categories = ['Restaurants', 'Shopping', 'Nightlife', 'Active Life', 'Beauty & Spas', 'Automotive', 'Home Services']
business_df = p.process_business(business_df, categories)

business_df.drop(['attributes', 'categories'], axis='columns', inplace=True)

business_df.isnull().mean().sort_values(ascending=False).head(10)

business_df.drop(['hours'], axis='columns', inplace=True)

business_df.dropna(subset=['latitude', 'longitude'], axis='rows', inplace=True)

business_df.drop(['address', 'business_id', 'name'], axis='columns', inplace=True)

business_df.drop(['city', 'neighborhood', 'postal_code'], axis='columns', inplace=True)

business_df = pd.get_dummies(business_df, drop_first=True)
y = business_df.is_open
X = business_df.drop(['is_open'], axis='columns')

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=.30, random_state=42)
# Random Forest Model
rf_model = RandomForestClassifier(random_state=42)
rf_model.fit(X_train, y_train)

# Model Evaluation
print('train accuracy: ', rf_model.score(X_train, y_train))
print('test accuracy: ', rf_model.score(X_test, y_test))

clf = RandomForestClassifier(random_state=42)

parameters = {'max_depth':[40],'min_samples_leaf':[1,2,5]}

scorer = make_scorer(f1_score)

grid_obj = GridSearchCV(clf, parameters, scoring=scorer)
grid_fit = grid_obj.fit(X, y)
best_clf = grid_fit.best_estimator_

best_clf.fit(X_train, y_train)
best_train_predictions = best_clf.predict(X_train)
best_test_predictions = best_clf.predict(X_test)

# f1_score
print('The training F1 Score is', f1_score(best_train_predictions, y_train))
print('The testing F1 Score is', f1_score(best_test_predictions, y_test))

best_clf

confusion_matrix(y_test, best_test_predictions)

tn, fp, fn, tp = confusion_matrix(y_test, best_test_predictions).ravel()
print(tn, fp, fn, tp)

feature_importances = pd.DataFrame(best_clf.feature_importances_,
                                   index = X_train.columns,
                                   columns=['importance']).sort_values('importance',
                                                                       ascending=False)
feature_importances.head(8)
a={'review_count':0.122037,'longitude':0.108432, 'stars':0.107642,'WiFi_free':0.076423,'latitude':0.058612,'BikeParking':0.026853}
dict = pd.DataFrame(pd.Series(a),colunmns = ['importance'])
dict = dict.reset_index().rename(colunmns = {'index':''})
dict.head

dic={'review_count':0.122037,'longitude':0.108432, 'stars':0.107642,'WiFi_free':0.076423,'latitude':0.058612,'BikeParking':0.026853,'AcceptsInsurance':0.024973,'Smoking_outdoor':0.014732}

data = pd.DataFrame([dic])
data = data.T
data.head()

df = pd.DataFrame.from_dict(dic, orient='index',columns=['importance'])
df = df.reset_index().rename(columns = {'index':''})


