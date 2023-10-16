'''
Copyright 2023 Starburst Data

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
'''

from pystarburst import Session, DataFrame
from pystarburst import functions as f
from pystarburst.functions import col

import trino
import env

import pyarrow as pa
import pandas as pd

# Class to handle connection to Starburst and retrieve data
class Data():
    """Class to handle connection to Starburst and retrieve data actions
    
    Attributes
    ----------
        host : str 
            Starburst host
        username : str 
            Starburst username
        password : str 
            Starburst password
        session_properties : dict 
            Starburst session properties
        session : pystarburst.Session
            Starburst session
        
        query_history : pystarburst.QueryHistor
            Starburst query history
        queries_list : list([str, str])
            List of Starburst queries
        
        initialized : bool
            Flag to indicate if data has been initialized
            
    Data Elements
    -------------
        segments : list(str)
            List of customer segments
        df_onprem_credit : pystarburst.DataFrame
            On-prem customer credit data
        df_dl_customer_360 : pystarburst.DataFrame
            Data lake customer data
        df_joined : pystarburst.DataFrame
            Joined on-prem and data lake customer data
        df_summary : pystarburst.DataFrame
            Summarized customer data

    Gradio Helpers
    --------------
        LOAD_CODE : str
            Code to load data
        SEGMENTS_CODE : str
            Code to get unique segments
        AGG_CODE : str
            Code to aggregate data
        WRITE_CODE : str
            Code to write data
    
    Methods
    --------------
        get_initial_data(do_agg = True)
            Get initial data
        refresh_session()
            Refresh Starburst session
        save_settings(h, u, p)
            Save Starburst settings and refresh session
        get_queries()
            Get Starburst queries
        get_unique_segs()
            Get unique customer segments
        get_agg_data(segments)
            Get aggregated customer data
        write_agg_data()
            Write aggregated customer data
        to_pyarrow(df: DataFrame) -> pa.Table
            Convert pystarburst.DataFrame to pyarrow.Table
        to_torch(t: pa.Table)
            Convert pyarrow.Table to torch.Tensor
    """
    INITIAL_CODE = """
# Imports
from pystarburst import Session, DataFrame
from pystarburst import functions as f
from pystarburst.functions import col
import trino

# Connect to Starburst
    
session_properties = {
    'host':host,
    'port': 443,
    'http_scheme': 'https',
    # See docs: https://github.com/trinodb/trino-python-client#authentication-mechanisms
    'auth': trino.auth.BasicAuthentication(username, password)
}

session = Session.builder.configs(self.session_properties).create()
"""

    LOAD_CODE = """


# Load Tables

df_onprem_credit = session.table('sep_dataproducts.customer_360.customer_with_credit')
df_dl_customer_360 = session.table('s3lakehouse.data_product_customer_360.customer_information')

# Clean & Join

df_onprem_credit = df_onprem_credit.with_column('risk_appetite',\\
        f.sql_expr("replace(risk_appetite, 'wild_west', 'very_low')"))

df_joined = df_onprem_credit.join(df_dl_customer_360,\\
        df_onprem_credit['custkey'] == df_dl_customer_360['custkey'])
    """
    
    SEGMENTS_CODE = """
segments = df_onprem_credit.select('customer_segment').distinct().to_pandas()['customer_segment'].to_list()
    """

    AGG_CODE = """
df_summary = \\
df_joined.filter(col('customer_segment').in_(segments))\\
.group_by('state', 'risk_appetite')\\
.count()\\
.sort(col('count').desc())
    """
    
    WRITE_CODE = """
# Clean up the target

session.sql("CREATE SCHEMA IF NOT EXISTS s3lakehouse.pystarburst_360_sum").collect()
session.sql("DROP TABLE IF EXISTS s3lakehouse.pystarburst_360_sum.s360_summary").collect()

# Write the data
df_joined.write.save_as_table(
    "s3lakehouse.pystarburst_360_sum.s360_summary"
)

# Validate the result
session.table("s3lakehouse.pystarburst_mis_sum.s360_summary").show()
"""

    host = env.HOST
    username = env.USERNAME
    password = env.PASSWORD

    session_properties = {
        "host":host,
        "port": 443,
        # Needed for https secured clusters
        "http_scheme": "https",
        # Setup authentication through login or password or any other supported authentication methods
        # See docs: https://github.com/trinodb/trino-python-client#authentication-mechanisms
        "auth": trino.auth.BasicAuthentication(username, password)
    }

    def __init__(self):
        # Setup a session, query history logger, and initial data frames
        if env.DEBUG: print("INFO: Data Init")
        self.session = Session.builder.configs(self.session_properties).create()

        self.query_history = self.session.query_history()
        self.queries_list = list()

        self.segments = ['silver', 'gold', 'platinum', 'bronze', 'diamond']
        
        self.initialized = False

    def get_initial_data(self, do_agg = True):
        if env.DEBUG: print("INFO: Get Initial Data")

        self.df_onprem_credit = self.session.table('sep_dataproducts.customer_360.customer_with_credit')
        self.df_dl_customer_360 = self.session.table('s3lakehouse.data_product_customer_360.customer_information')

        self.df_onprem_credit = self.df_onprem_credit.with_column('risk_appetite', f.sql_expr("replace(\"risk_appetite\", 'wild_west', 'very_low')"))
        self.df_joined = self.df_onprem_credit.join(self.df_dl_customer_360, self.df_onprem_credit['custkey'] == self.df_dl_customer_360['custkey'])

        self.segments = self.df_onprem_credit.select('customer_segment').distinct().to_pandas()['customer_segment'].to_list()
        
        if do_agg:
            self.get_agg_data(self.segments)
        
        self.initialized = True
    
    def refresh_session(self):
        self.session = Session.builder.configs(self.session_properties).create()

    def save_settings(self, h, u, p):
        self.host = h
        self.username = u
        self.password = p
        self.refresh_session()

    def get_queries(self) -> list:
        if not self.initialized:
            self.get_initial_data(do_agg=False)
        
        return pd.DataFrame(self.query_history.queries)

    def get_unique_segs(self) -> list[str]:
        print("INFO: Get Unique Segs")
        if not self.initialized:
            self.get_initial_data(do_agg=False)
        
        return self.segments

    def get_agg_data(self, segments) -> pd.DataFrame:
        # Summary
        print("INFO: Get Agg Data")
        if not self.initialized:
            self.get_initial_data(do_agg=False)
        
        self.df_summary = self.df_joined.filter(col('customer_segment').in_(segments))\
        .group_by('state', 'risk_appetite')\
        .count()\
        .sort(col('count').desc())

        #self.df_summary.show()
        return self.df_summary.to_pandas()
    
    def write_agg_data(self):
        if env.DEBUG: print("INFO: Write Agg Data")
        if not self.initialized:
            self.get_initial_data()
        self.session.sql("CREATE SCHEMA IF NOT EXISTS s3lakehouse.pystarburst_360_sum").collect()

        self.session.sql("DROP TABLE IF EXISTS s3lakehouse.pystarburst_360_sum.s360_summary").collect()

        self.df_summary.write.save_as_table(
            "s3lakehouse.pystarburst_360_sum.s360_summary",
        )
        new_table = self.session.table("s3lakehouse.pystarburst_360_sum.s360_summary")

        return new_table.to_pandas()

    def to_pyarrow(df: DataFrame) -> pa.Table:
        return pa.deserialize_pandas(df.to_pandas())
