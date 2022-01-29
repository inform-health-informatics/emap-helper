#!/usr/bin/env python
# coding: utf-8

# # HyMind Lab Example

# <img src="https://upload.wikimedia.org/wikipedia/commons/1/17/Warning.svg" width="20"/> ***Please make a copy of this notebook and do not edit in place***

# See the [system diagram](https://github.com/HYLODE/HyLevel/blob/main/system-design/HYLODE.png?raw=True) for an overview of the HYLODE system components referenced in this notebook.  
# (_You will need to be signed into GitHub to view_)

# ## Packages

# ### Available

# In[1]:


import pkg_resources
installed_packages = pkg_resources.working_set
installed_packages_list = sorted([f'{i.key}=={i.version}' for i in installed_packages])


# In[2]:


# Uncomment to list installed packages
# installed_packages_list


# ### Need more?

# For a quick installation, uncomment & run the command below (replace `ujson` with the package you want)

# In[3]:


# !pip install ujson


# In[4]:


# import ujson
# ujson.__version__


# **Packages installed this way will disappear when the container is restarted.**

# To have the package permanently available, please log a ticket on [ZenHub](https://app.zenhub.com/workspaces/hysys-5fd9bdd7e234b8001e6ed46b/board?repos=321907911) requesting the package to be added to the HyMind Lab.

# ### Some imports

# In[5]:


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


# ### Constants

# In[ ]:


ward = 'T03'


# ----

# # Database Connections

# ## EMAP DB

# Access to EMAP is required for multiple components of the system to function properly.  
# This includes some of the functions that are useful to run in the HyMind Lab

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


# In[ ]:


uds_host = os.getenv('EMAP_DB_HOST')
uds_name = os.getenv('EMAP_DB_NAME')
uds_port = os.getenv('EMAP_DB_PORT')
uds_user = os.getenv('EMAP_DB_USER')
uds_passwd = os.getenv('EMAP_DB_PASSWORD')


# Create a SQLAlchemy Engine for accessing the UDS:

# In[ ]:


emapdb_engine = create_engine(f'postgresql://{uds_user}:{uds_passwd}@{uds_host}:{uds_port}/{uds_name}')


# ## HYLODE DB

# The `hylode` database is a containerised instance of Postgres 12 used by the various components to store data for use further down the pipeline.  
# You can think of it as the _medium of data flow_ for our system.  
# Unlike the `uds`, it is private to us.  
# You don't need individual credentials, everthing is baked into the environment variables.
# 
# There are several schemas, roughly one for each subsystem (see link to system diagram above).
# 
# Storing data in and retrieving data from the `hylode` database happens through the APIs provided by the `hyflow`, `hygear` & `hycastle` modules.  
# Direct interaction with the database is not an expected part of the HyMind workflow and presented here for interest only.

# In[ ]:


db_host = os.getenv('HYLODE_DB_HOST')
db_name = os.getenv('HYLODE_DB_NAME')
db_user = os.getenv('HYLODE_DB_USER')
db_passwd = os.getenv('HYLODE_DB_PASSWORD')
db_port = os.getenv('HYLODE_DB_PORT')                                                                                                       


# In[ ]:


hydb_engine = create_engine(f'postgresql://{db_user}:{db_passwd}@{db_host}:{db_port}/{db_name}')


# ### HyDef
# *The `hydef` schema in the `hylode` database contains static reference data*

# #### Locations

# In[ ]:


beds_df = pd.read_sql(
    """
    select 
        bed.id
        ,bed.code
        ,bed.source_id
        ,bay.code
        ,bay.type
        ,ward.code
        ,ward.name
        ,ward.type
    from
        hydef.beds bed
    inner join hydef.bays bay on bed.bay_id = bay.id
    inner join hydef.wards ward on ward.code = bay.ward_code
    order by ward.code, bay.code, bed.code
    """,
    hydb_engine
)


# In[ ]:


beds_df


# #### ICU Observation Types Catalogue
# 
# A growing list of observation types that we are interested in for the ICU pipeline

# In[ ]:


icu_obs_types = pd.read_sql('select * from hydef.icu_observation_types', hydb_engine)


# In[ ]:


icu_obs_types


# ----

# # HyCastle

# *The HyCastle component is responsible for serving features for both training and prediction.*
# 
# HyCastle provides a high level interface to getting clean, transformed features & labels from the pipeline.
# 
# To dig deeper, the `hyflow` & `hygear` packages are also available for use within the HyMind Lab.  
# Additionally, all the tables in the `hydef`, `hyflow` & `hygear` schemas in the `hylode` database can be accessed directly using the database connection defined above, just like the `star` schema on the `uds` 🙂.

# In[ ]:


from hycastle.icu_store import SITREP_FEATURES
from hycastle.icu_store.retro import retro_dataset
from hycastle.icu_store.live import live_dataset, emap_snapshot # <-- contains PII


# ## Retrospective Data

# In[ ]:


training_df = retro_dataset(ward)


# In[ ]:


training_df.shape


# In[ ]:


training_df.head()


# In[ ]:


training_df.columns


# In[ ]:


training_df.episode_slice_id.duplicated().any()


# In[ ]:


training_df.isnull().any()


# ## Live Data
# 
# **These functions return personally identifiable information**

# ### HyLode Live Episode Slices

# In[ ]:


prediction_df = live_dataset(ward)


# In[ ]:


prediction_df.shape


# In[ ]:


prediction_df


# ### EMAP Live Census Snapshot

# In[ ]:


emap_df = emap_snapshot(ward)


# In[ ]:


emap_df.head()


# ### Filter
# Limit episode slices used for prediction to admissions that are in the EMAP census

# In[ ]:


prediction_df.csn.isin(emap_df.csn)


# In[ ]:


prediction_df = prediction_df[prediction_df.csn.isin(emap_df.csn)]


# In[ ]:


prediction_df


# ----

# # HyFlow
# *The HyFlow component is responsible for ingesting raw data from upstream data sources such as EMAP & Caboodle.*
# 
# The idea is for HyMind Lab users to operate at the level of **HyCastle** and not to have to worry about the raw data ingress during the modelling process.  
# Access to source data is still required for exploration and feature engineer which can be done through the `hyflow` package which is available for use from within the HyMind Lab.   
# Additionally, all the tables in the `hyflow` schema in the `hylode` database can be accessed directly.

# In[ ]:


from hyflow.fetch.hydef import beds_from_hydef, icu_observation_types_from_hydef
from hyflow.fetch.icu_episode_slices import icu_episode_slices_from_emap, icu_episode_slices_from_hyflow
from hyflow.fetch.icu_patients import icu_patients_from_emap
from hyflow.fetch.icu_observations import icu_observations_from_emap


# ## Minimal example using the HyFlow package

# #### Fetch ICU Episode Slices from EMAP

# In[ ]:


# the point-in-time we are interested in:  7am on 17/07/2021 BST
horizon_dt = datetime(2021, 7, 17, 7, 0, 0).astimezone(LONDON_TZ)


# In[ ]:


beds_df = beds_from_hydef(ward)


# In[ ]:


episode_slices_df = icu_episode_slices_from_emap(ward, horizon_dt, list(beds_df.hl7_location))


# The HyFlow method adds the `episode_key` as that is a HYLODE concept and _not_ available in EMAP.

# In[ ]:


episode_slices_df


# In[ ]:


# Attach HyDef bed_id to episode slice & drop HL7 location string
episode_slices_df = episode_slices_df.join(
    beds_df.loc[:, ['bed_id', 'hl7_location']].set_index('hl7_location'),
    on='hl7_location'
).drop(columns=['hl7_location'])


# In[ ]:


episode_slices_df


# #### Fetch matching Patients from EMAP for Episode Slices that are in in HyFlow

# In[ ]:


# the point-in-time we are interested in:  8pm on 17/07/2021 BST
horizon_dt = datetime(2021, 7, 17, 20, 0, 0).astimezone(LONDON_TZ)


# In[ ]:


# get our saved Episode Slices
episode_slices_df = icu_episode_slices_from_hyflow(ward, horizon_dt)


# In[ ]:


episode_slices_df.head()


# In[ ]:


patients_df = icu_patients_from_emap(ward, horizon_dt, list(episode_slices_df.csn))


# In[ ]:


patients_df


# In[ ]:


# Attach HyFlow episode_slice_id to patient
patients_df = patients_df.join(
    episode_slices_df.loc[:, ['episode_slice_id', 'csn']].set_index('csn'),
    on='csn'
).drop(columns=['csn'])


# In[ ]:


patients_df


# #### Fetch matching Observations of interest in EMAP for Episode Slices that are in HyFlow

# In[ ]:


lookback_hrs = 24 # size of the trailing window we are interested in


# In[ ]:


# the point-in-time we are interested in:  10am on 17/07/2021 BST
horizon_dt = datetime(2021, 7, 17, 10, 0, 0).astimezone(LONDON_TZ)


# In[ ]:


# get our saved Episode Slices
episode_slices_df = icu_episode_slices_from_hyflow(ward, horizon_dt)


# In[ ]:


episode_slices_df.head()


# In[ ]:


# get our reference list of observation types we care about
obs_types_df = icu_observation_types_from_hydef()


# In[ ]:


obs_types_df.head()


# In[ ]:


obs_df = icu_observations_from_emap(
    ward,
    horizon_dt,
    list(episode_slices_df.csn),
    list(obs_types_df.source_id),
    lookback_hrs
)


# In[ ]:


obs_df


# In[ ]:


# Attach HyDef observation type id to observation
obs_df = obs_df.join(
    obs_types_df.loc[:, ['obs_type_id', 'source_id']].set_index('source_id'),
    on='source_id'
)


# In[ ]:


# Attach HyFlow episode_slice_id to observation
obs_df = obs_df.join(
    episode_slices_df.loc[:, ['episode_slice_id', 'csn']].set_index('csn'),
    on='csn'
).drop(columns=['csn'])


# In[ ]:


obs_df


# ## Accessing the `hyflow` schema directly

# **Directly querying the `hylode` database will return personally identifiable information**
# 
# Like most tables in the `hylode` database, the `hyflow` schema tables are all **_immutable logs_**.  
# That means data is only ever appended, never updated in place.
# 
# This also means, for example, that an individual patient will have many records in the `icu_patients_log` table,   
# one for each slice that was taken while their episode was active.
# 
# **Important notes about direct Hylode DB access:**  
# The queries provided through the functions in the `hyflow` & `hygear` packages take care of removing duplicates.  
# If you access the schemas directly you will need to do that yourself - see the various `hyflow__*.sql` files in `hygear/load/sql` for examples of partitioning over `episode_slice_id` and `horizon_dt` columns.
# 
# Other conveniences are provided by the packages.  
# For example, the Postgres/SQLAlchemy/Pandas stack does not support storing timedeltas directly (even though it is a supported data type in both Postgres & Pandas, SQLAlchemy is unable to handle it).  
# That means the raw `span` column in the `hyflow.icu_episode_slices_log ` table is in nanoseconds.  
# Converting to a timedelta is done in the packages but you'll have to do that yourself if you access the raw tables.

# ### ICU Episode Slices with Bed Id

# In[ ]:


sql_episode_slices_df = pd.read_sql(
    '''
        select 
            ep.id AS episode_slice_id
            , ep.episode_key
            , ep.csn
            , ep.admission_dt
            , ep.discharge_dt
            , beds.source_id AS bed
            , horizon_dt
            , log_dt
        from hyflow.icu_episode_slices_log ep
            inner join hydef.beds beds ON ep.bed_id = beds.id
            inner join hydef.bays bays ON beds.bay_id = bays.id
         WHERE bays.ward_code = %(ward)s
        order by episode_key, horizon_dt limit 1000
    ''', 
    hydb_engine,
    params={'horizon_dt': horizon_dt, 'ward': 'T03'}
)


# In[ ]:


sql_episode_slices_df


# ### ICU Patients

# In[ ]:


sql_patients_df = pd.read_sql('''
    select 
        id AS patient_log_id
        , episode_slice_id
        , mrn
        , name
        , dob
        , sex
        , ethnicity
        , postcode
        , horizon_dt
        , log_dt
    from hyflow.icu_patients_log 
    order by mrn, horizon_dt limit 1000''', 
    hydb_engine
)


# In[ ]:


sql_patients_df


# ### ICU Observations

# In[ ]:


sql_obs_df = pd.read_sql('select * from hyflow.icu_observations_log order by episode_slice_id, horizon_dt limit 1000', hydb_engine)


# In[ ]:


sql_obs_df


# ----

# # HyGear
# *The HyGear component is responsible for transforming raw data collected by HyFlow into features that will be consumed by the models in HyMind*.
# 
# The idea is that explorative feature engineering happens on source data from HyFlow or EMAP in the HyMind Lab.  
# Once the transformation has been refined, it is added to the `hygear` package.  
# 
# The `hygear` package itself is available for use from within the HyMind Lab.
# 
# Similarly, all the tables in the `hygear` schema in the `hylode` database can be accessed directly.  
# Like the `hyflow` tables shown above, these are also in the _immutable log_ style.

# In[ ]:


from hygear.load.hydef import icu_observation_types_from_hydef
from hygear.load.hyflow import icu_episode_slices_from_hyflow, icu_patients_from_hyflow, icu_observations_from_hyflow
from hygear.load.hygear import icu_features_from_hygear

from hygear.transform.cog1.icu_therapeutics import (
    InotropeTransformer,
    NitricTransformer,
    RenalTherapyTransformer
)
from hygear.transform.cog1.icu_patient_state import (
    AgitationTransformer,
    DischargeStatusTransformer,
    PronedTransformer
)
from hygear.transform.cog1.icu_ventilation import TracheostomyTransformer, VentilationTypeTransformer
from hygear.transform.cog1.icu_vitals import (
    HeartRateTransformer,
    RespiratoryRateTransformer,
    TemperatureTransformer
)
from hygear.transform.cog1.icu_temporal import AdmissionAgeTransformer, LengthOfStayTransformer, DischargeLabelTransformer
from hygear.transform.cog2.icu_work_intensity import WorkIntensityTransformer


# ## Single Horizon Example

# In[ ]:


# the point-in-time we are interested in:  10pm on 31/08/2021 BST
horizon_dt = datetime(2021, 8, 31, 22, 0, 0).astimezone(LONDON_TZ)


# ### Fetch ICU Episode Slices active at a specific point-in-time

# In[ ]:


episode_slices_df = icu_episode_slices_from_hyflow(
    ward,
    horizon_dt
)


# In[ ]:


episode_slices_df


# ### Fetch matching Patients for ICU Episode Slices active at a specific point-in-time

# In[ ]:


# fetch matching patients
patients_df = icu_patients_from_hyflow(
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


patients_df.head()


# In[ ]:


# join episode slices with patients
combined_df = episode_slices_df.join(
        patients_df.loc[:, ['episode_slice_id', 'mrn', 'dob', 'sex', 'ethnicity']].set_index('episode_slice_id'),
        on='episode_slice_id'
    ).drop(['log_dt', 'horizon_dt'], axis=1)


# In[ ]:


combined_df.head()


# ### Fetch matching Observations for ICU Episode Slices active at a specific point-in-time
# _this is in long format, multiple rows per_ `episode_slice_id` 

# In[ ]:


# number of trailing hours we are interested in
lookback_hrs = 24


# In[ ]:


# fetch matching observations
obs_df = icu_observations_from_hyflow(
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id),
    lookback_hrs
)


# In[ ]:


obs_df.head()


# In[ ]:


# fetch the observation types reference catalogue
obs_types_df = icu_observation_types_from_hydef()


# In[ ]:


# join observations with metadata
obs_df = obs_df.join(
    obs_types_df.set_index('obs_type_id'),
    on='obs_type_id'
)


# In[ ]:


obs_df.head()


# In[ ]:


# join observations with episode slices to get episode key
eps_obs_df = obs_df.join(
    episode_slices_df.loc[:, ['episode_slice_id', 'episode_key', 'admission_dt', 'discharge_dt']].set_index('episode_slice_id'),
    on='episode_slice_id'
)


# In[ ]:


eps_obs_df.groupby('episode_key')['obs_id'].count().rename('n_observations')


# ### Fetch generated ICU Patient State Features for ICU Episode Slices active at a specific point-in-time

# In[ ]:


patient_state_df = icu_features_from_hygear(
    'patient_state',
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


patient_state_df


# In[ ]:


# join with patient state
combined_df = combined_df.join(
        patient_state_df.loc[:, ['episode_slice_id', 'is_proned_1_4h', 'discharge_ready_1_4h', 'is_agitated_1_8h']].set_index('episode_slice_id'),
        on='episode_slice_id'
    )


# In[ ]:


combined_df.head()


# ### Fetch generated ICU Therapeutics Features for ICU Episode Slices active at a specific point-in-time

# In[ ]:


therapeutics_df = icu_features_from_hygear(
    'therapeutics',
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


therapeutics_df


# In[ ]:


# join with therapeutics
combined_df = combined_df.join(
        therapeutics_df.loc[:, ['episode_slice_id', 'n_inotropes_1_4h', 'had_nitric_1_8h', 'had_rrt_1_4h']].set_index('episode_slice_id'),
        on='episode_slice_id'
    )


# In[ ]:


combined_df.head()


# ### Fetch generated ICU Ventilation Features for ICU Episode Slices active at a specific point-in-time

# In[ ]:


ventilation_df = icu_features_from_hygear(
    'ventilation',
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


ventilation_df


# In[ ]:


# join with ventilation
combined_df = combined_df.join(
        ventilation_df.loc[:, ['episode_slice_id', 'had_trache_1_12h', 'vent_type_1_4h']].set_index('episode_slice_id'),
        on='episode_slice_id'
    )


# In[ ]:


combined_df.head()


# ### Fetch generated ICU Vitals Features for ICU Episode Slices active at a specific point-in-time

# In[ ]:


vitals_df = icu_features_from_hygear(
    'vitals',
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


vitals_df


# In[ ]:


# join with vitals
combined_df = combined_df.join(
        vitals_df.loc[:, ['episode_slice_id', 'avg_heart_rate_1_24h', 'max_temp_1_12h', 'avg_resp_rate_1_24h']].set_index('episode_slice_id'),
        on='episode_slice_id'
    )


# In[ ]:


combined_df.head()


# ### Fetch generated ICU Work Intensity Metric Features for ICU Episode Slices active at a specific point-in-time

# In[ ]:


wim_df = icu_features_from_hygear(
    'work_intensity',
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


wim_df


# In[ ]:


# join with work intensity
combined_df = combined_df.join(
        wim_df.loc[:, ['episode_slice_id', 'wim_1']].set_index('episode_slice_id'),
        on='episode_slice_id'
    )


# In[ ]:


combined_df.head()


# ### Fetch generated ICU Temporal Features for ICU Episode Slices active at a specific point-in-time

# In[ ]:


temporal_df = icu_features_from_hygear(
    'temporal',
    ward,
    horizon_dt,
    list(episode_slices_df.episode_slice_id)
)


# In[ ]:


temporal_df


# In[ ]:


# join with temporal
combined_df = combined_df.join(
        temporal_df.loc[:, ['episode_slice_id', 'elapsed_los_td', 'total_los_td', 'remaining_los_td',]].set_index('episode_slice_id'),
        on='episode_slice_id'
    )


# In[ ]:


combined_df.head()


# ### Combined

# In[ ]:


combined_df.shape


# In[ ]:


combined_df.head()


# ----

# In[ ]:




