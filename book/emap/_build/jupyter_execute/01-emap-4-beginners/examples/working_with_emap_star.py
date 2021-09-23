#!/usr/bin/env python
# coding: utf-8

# # Working with EMAP star

# A template JupyterNotebook for working with EMAP. The following features of this notebook, and associated files are documented here to minimise the risk of data leaks or other incidents.
# 
# - Usernames and passwords are stored in a .env file that is excluded from version control. The example `env` file at `./config/env` should be edited and saved as `./config/.env`. A utility function `load_env_vars()` is provided that will confirm this file exists and load the configuration into the working environment.
# - .gitattributes are set to strip JupyterNotebook cells when pushing to GitHub

# ## Basic set-up

# Load libraries

# In[1]:


import os
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

from pathlib import Path
from sqlalchemy import create_engine
from utils.setup import load_env_vars


# Load environment variables and set-up SQLAlchemy connection engine for the EMAP Star

# In[ ]:


# Load environment variables
load_env_vars()

# Construct the PostgreSQL connection
uds_host = os.getenv('EMAP_DB_HOST')
uds_name = os.getenv('EMAP_DB_NAME')
uds_port = os.getenv('EMAP_DB_PORT')
uds_user = os.getenv('EMAP_DB_USER')
uds_passwd = os.getenv('EMAP_DB_PASSWORD')

emapdb_engine = create_engine(f'postgresql://{uds_user}:{uds_passwd}@{uds_host}:{uds_port}/{uds_name}')


# The above code is also abstracted into a function (below) but shown in long form above to make clear what we are doing.
# ```python
# from utils.setup import make_emap_engine
# emapdb_engine = make_emap_engine
# ```

# Now use the connection to work with EMAP.\
# 
# For example, let's inspect patients currently in ED or Resus.
# 
# Here's the SQL:
# 
# ```sql
# -- Example script 
# -- to pick out patients currently in A&E resus or majors
# 
# SELECT
#    vd.location_visit_id
#   ,vd.hospital_visit_id
#   ,vd.location_id
#   -- ugly HL7 location string 
#   ,lo.location_string
#   -- time admitted to that bed/theatre/scan etc.
#   ,vd.admission_time
#   -- time discharged from that bed
#   ,vd.discharge_time
# 
# FROM star.location_visit vd
# -- location label
# INNER JOIN star.location lo ON vd.location_id = lo.location_id
# WHERE 
# -- last few hours
# vd.admission_time > NOW() - '12 HOURS'::INTERVAL	
# -- just CURRENT patients
# AND
# vd.discharge_time IS NULL
# -- filter out just ED and Resus or Majors
# AND
# -- unpacking the HL7 string formatted as 
# -- Department^Ward^Bed string
# SPLIT_PART(lo.location_string,'^',1) = 'ED'
# AND
# SPLIT_PART(lo.location_string,'^',2) ~ '(RESUS|MAJORS)'
# -- sort
# ORDER BY lo.location_string
# ;
# 
# ```
# 

# The SQL script is stored at `./snippets/sql-vignettes/current_bed.sql`.\
# We can load the script, and read the results into a Pandas dataframe.
# 

# In[ ]:


# Read the sql file into a query 'q' and the query into a dataframe
q = Path('./snippets/sql-vignettes/current_bed.sql').read_text()
df = pd.read_sql_query(q, emapdb_engine)


# In[ ]:


df.head()


# In[ ]:




