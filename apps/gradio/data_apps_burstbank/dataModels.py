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

from dotenv import load_dotenv
load_dotenv("./.env")
import env

import pyarrow as pa
import pandas as pd

# Class to handle connection to Starburst and retrieve data
class Data():
    # Setup connection to Starburst
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
        "auth": trino.auth.OAuth2Authentication()
    }

    def __init__(self):
        # Setup a session, query history logger, and initial data frames
        if env.DEBUG: print("INFO: Data Init")
        self.session = Session.builder.configs(self.session_properties).create()

        self.query_history = self.session.query_history()
        self.queries_list = list()

        self.segments = ['silver', 'gold', 'platinum', 'bronze', 'diamond']
        self.risk_appetites = ['wild_west', 'conservative', 'low', 'high', 'medium']
        self.states = ['AK', 'AL', 'AR', 'AZ', 'CA', 'CO', 'CT', 'DE', 'FL', 'GA', 'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA', 'MD', 'ME', 'MI', 'MN', 'MO', 'MS', 'MT', 'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH', 'OK', 'OR', 'PA', 'RI', 'SC', 'SD', 'TN', 'TX', 'UT', 'VA', 'VT', 'WA', 'WI', 'WV', 'WY']
        
        self.initialized = False

    def get_initial_data(self, do_agg = True):
        if env.DEBUG: print("INFO: Get Initial Data")

        # Load Tables
        self.df_account = self.session.table(f'{env.CATALOG}.{env.SCHEMA}.account')
        self.df_customer = self.session.table(f'{env.CATALOG}.{env.SCHEMA}.customer')
        self.df_customer_profile = self.session.table(f'{env.CATALOG}.{env.SCHEMA}.customer_profile')

        self.df_joined_account = self.df_account.join(self.df_customer, self.df_account['custkey'] == self.df_customer['custkey'])
        self.df_joined_account = self.df_joined_account.join(self.df_customer_profile, self.df_account['custkey'] == self.df_customer_profile['custkey'])

        if do_agg:
            self.get_agg_data(self.segments, self.risk_appetites, self.states)
        
        self.initialized = True
    
    def refresh_session(self):
        self.session = Session.builder.configs(self.session_properties).create()

    def save_settings(self, h):
        self.host = h
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

    def filter_accounts(self, segments, risks, states) -> pd.DataFrame:
        print("INFO: Filter Accounts")
        if not self.initialized:
            self.get_initial_data(do_agg=False)
        
        if risks is None:
            risks = self.risk_appetites

        if states is None:
            states = self.states

        self.accounts = self.df_joined_account = self.df_joined_account.filter(col('customer_segment').in_(segments))\
        .filter(col('risk_appetite').in_(risks))\
        .filter(col('state').in_(states))

        return self.accounts.to_pandas()
    
    def get_agg_data(self, segments, risks, states) -> pd.DataFrame:
        # Summary
        print("INFO: Get Agg Data")
        if not self.initialized:
            self.get_initial_data(do_agg=False)
        
        if risks is None:
            risks = self.risk_appetites

        if states is None:
            states = self.states

        self.df_summary = self.df_joined_account.filter(col('customer_segment').in_(segments))\
        .filter(col('risk_appetite').in_(risks))\
        .filter(col('state').in_(states))\
        .group_by('state', 'risk_appetite')\
        .count()\
        .sort(col('count').desc())

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
