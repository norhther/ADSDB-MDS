# -*- coding: utf-8 -*-
"""Untitled2.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1QjosDbQocdDYVYce6CQmE_QvNYptQybP
"""

from google.colab import drive
drive.mount('/content/drive/')

!pip install pycountry
!pip install featurewiz
!pip install xgboost
!pip install seaborn
!pip install -U scikit-learn

import pandas as pd
import numpy as np
import pycountry
import seaborn as sns
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler 
from sklearn.ensemble import RandomForestClassifier 
from sklearn.linear_model import LogisticRegression 
from sklearn.metrics import accuracy_score 
from featurewiz import featurewiz

# Load dataset
allData = pd.read_csv("drive/MyDrive/ADSDB2-Testing/feature_generated.csv")
print(allData)

np.random.seed(1234)
allData.shape
target = 'class'
features, train = featurewiz(allData, target, corr_limit=0.7, verbose=2, sep=",",
header=0,test_data="", feature_engg="", category_encoders="")

# Here we see the most imprtant features
# which are ['finalIq', 'country_name', 'internet', 'math', 'computer_n', 'room', 'mother_educ']
print(features)
print(train)