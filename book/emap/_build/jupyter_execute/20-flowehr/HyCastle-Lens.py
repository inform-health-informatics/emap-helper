#!/usr/bin/env python
# coding: utf-8

# # HyCastle Lens Example

# <img src="https://upload.wikimedia.org/wikipedia/commons/1/17/Warning.svg" width="20"/> ***Please make a copy of this notebook and do not edit in place***

# See the [system diagram](https://github.com/HYLODE/HyLevel/blob/main/system-design/HYLODE.png?raw=True) for an overview of the HYLODE system components referenced in this notebook.  
# (_You will need to be signed into GitHub to view_)

# ### Some imports

# In[1]:


from datetime import datetime, timedelta
import os
from pathlib import Path
from pprint import pprint
import urllib

import arrow
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sqlalchemy import create_engine

from hylib.dt import LONDON_TZ, convert_dt_columns_to_london_tz
from hycastle.lens.icu import ICUSitRepUiLens, ICUSitRepLoSLens


# ### Constants

# In[ ]:


ward = 'T03'


# ----

# ## Setup EMAP DB

# Access to EMAP is required for HyCastle to function properly.  

# ### EMAP credentials

# EMAP credentials are allocated per user and not stored in the environment variables.
# You do not want your credentials to leak into the source repository.
# 
# One way of safeguarding is to create a file called `secret` at the top level of the **HyMind** repository (next to this notebook).   
# Do this here in Jupyter and not a local copy of the repo.  
# 
# The first line should be your UDS **username** and the second line should be your UDS **password**.
# 
# `secret` has been added to `.gitignore` and will be excluded from the repository.

# Read your username & password into the environment:

# In[ ]:


os.environ['EMAP_DB_USER'], os.environ['EMAP_DB_PASSWORD'] = Path('secret').read_text().strip().split('\n')


# -----

# # HyCastle

# *The HyCastle component is responsible for serving features for both training and prediction.*
# 
# HyCastle provides a high level interface to getting clean, transformed features & labels from the pipeline.

# In[ ]:


from hycastle.icu_store.retro import retro_dataset
from hycastle.icu_store.live import live_dataset # <-- includes PII


# ## Retrospective Training Data

# In[ ]:


train_df = retro_dataset(ward)


# In[ ]:


train_df.shape


# In[ ]:


train_df.head()


# ## Live Data for Prediction

# In[ ]:


predict_df = live_dataset(ward)


# In[ ]:


predict_df.shape


# In[ ]:


predict_df.head()


# ----

# # Lens
# 
# A Lens represents a subset of features from a pipeline as well as the pre-processing steps needed for each feature.

# ## Example LoS Model Lens

# In[ ]:


lens = ICUSitRepLoSLens()


# #### Fit on Training Data
# Focus the Lens on the training dataset

# In[ ]:


processed_train_df = lens.fit_transform(train_df)


# In[ ]:


processed_train_df.shape


# In[ ]:


processed_train_df.head()


# In[ ]:


processed_train_df.dtypes


# In[ ]:


processed_train_df.isnull().any()


# #### Transform Prediction Data
# Use the Lens that was fitted on the training dataset to transform the prediction dataset

# In[ ]:


predict_df.head()


# In[ ]:


processed_predict_df = lens.transform(predict_df)


# In[ ]:


processed_predict_df.head()


# In[ ]:


processed_predict_df.dtypes


# In[ ]:


processed_predict_df.isnull().any()


# Live prediction dataset doesn't have `discharge_dt`s so `total_los_td` & `remaining_los_td` are unavailable

# ## SitRep UI Lens

# In[ ]:


lens = ICUSitRepUiLens()


# In[ ]:


output_df = lens.fit_transform(predict_df)


# In[ ]:


output_df.head()


# In[ ]:


output_df.dtypes


# 
# ----

# ## Crafting a new Lens

# ### Required imports

# In[ ]:


from typing import *
import numpy as np
import pandas as pd
from sklearn.compose import make_column_selector
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import StandardScaler, OneHotEncoder, OrdinalEncoder
from sklearn.pipeline import Pipeline as SKPipeline
from sklearn.impute import MissingIndicator, SimpleImputer

from hycastle.icu_store import SITREP_FEATURES
from hycastle.lens.base import BaseLens
from hycastle.lens.icu import *


# ### Available features
# The following features are available in the ICU SitRep Pipeline

# In[ ]:


SITREP_FEATURES


# ### Definintion

# In[ ]:


class CustomICUSitRepLens(BaseLens):
    """
    Lens to focus ICU SitRep data 
    """
    
    # Whether to convert all columns in the output dataframe to floating point
    numeric_output = True
    
    # Select a subset of SITREP_COLUMNS to include in this Lens
    @property
    def input_cols(self) -> List[str]:
        return [
            'episode_slice_id',
            'episode_key',
            'admission_dt',
            'bay_type',
            'sex',
            'ethnicity',
            'admission_age_years',
            'avg_heart_rate_1_24h',
            'discharge_ready_1_4h',
            'n_inotropes_1_4h',
            'vent_type_1_4h',
            'wim_1',
            'elapsed_los_td',
            'horizon_dt'
        ]

    specification = ColumnTransformer(
        [
            (
                # Subset of columns that require no pre-processing and can be passed through as is
                'select',
                'passthrough',
                [
                    'episode_slice_id',
                    'admission_age_years',
                    'n_inotropes_1_4h',
                    'wim_1',
                ]
            ),
            # Pre-processing operations to apply to columns
            # Input is 3-element tuple:
            #   - name for pre-processing step
            #   - transformer instance
            #   - list of columns to apply to
            # Any Scikit-Learn Transformer type is valid.
            # Scikit-Learn Pipeline can also be used to combine multiple transformation for a single column, applied sequentially.
            # The only caveat is that when a SK Pipeline is used, any transformation that creates new columns, e.g. adding a missing indicator column or
            # one-hot-encoding, it must be the last step in the Pipeline.
            # Details & examples for implementing custom pre-processing transformations are in `hycastle.lens.utils` 
            (
                'admission_dt_exp',
                DateTimeExploder(),
                ['admission_dt']
            ),
            (
                'bay_type_enc',
                OneHotEncoder(),
                ['bay_type']
            ),
            (
                'sex_enc',
                OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1),
                ['sex']
            ),
            (
                'ethnicity_miss',
                MissingIndicator(
                    features='all',
                    missing_values=None
                ),
                ['ethnicity']
            ),
            (
                'ethnicity_enc',
                OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1),
                ['ethnicity']
            ),
            (
                'discharge_ready_1_4h_enc',
                OneHotEncoder(handle_unknown='ignore'),
                ['discharge_ready_1_4h']
            ),
            (
                'vent_type_1_4h_enc',
                OrdinalEncoder(handle_unknown='use_encoded_value', unknown_value=-1),
                ['vent_type_1_4h']
            ),
            (
                'avg_heart_rate_1_24h_prep',
                SKPipeline(
                    steps=[

                        (
                            'avg_heart_rate_1_24h_scale',
                            StandardScaler(),
                        ),
                        (
                            'avg_heart_rate_1_24h_impute',
                            SimpleImputer(strategy='mean', add_indicator=True),
                        ),
                    ]
                ),
                ['avg_heart_rate_1_24h']
            ),
            (
                'elapsed_los_td_seconds',
                FunctionTransformer(timedelta_as_hours),
                ['elapsed_los_td']
            ),
            (
                'horizon_dt_exp',
                DateTimeExploder(),
                ['horizon_dt']
            ),
        ]
    )


# #### Application

# In[ ]:


lens = CustomICUSitRepLens()


# In[ ]:


tdf = lens.fit_transform(train_df)


# In[ ]:


tdf


# In[ ]:


pdf = lens.transform(predict_df)


# In[ ]:


pdf


# In[ ]:





# In[ ]:





# In[ ]:




