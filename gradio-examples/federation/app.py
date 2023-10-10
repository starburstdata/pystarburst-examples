import gradio as gr
from datetime import date
from dateutil.relativedelta import relativedelta
import pandas as pd

pd.options.plotting.backend = "plotly"

from pystarburst import Session
from pystarburst import functions as f
from pystarburst.functions import col

import trino

from env import *

host = HOST
username = USERNAME
password = PASSWORD

session_properties = {
    "host":host,
    "port": 443,
    # Needed for https secured clusters
    "http_scheme": "https",
    # Setup authentication through login or password or any other supported authentication methods
    # See docs: https://github.com/trinodb/trino-python-client#authentication-mechanisms
    "auth": trino.auth.BasicAuthentication(username, password)
}


class Data():

    def __init__(self):
        # Setup a session, query history logger, and initial data frames
        session = Session.builder.configs(session_properties).create()

        self.df_onprem_credit = session.table('sep_dataproducts.customer_360.customer_with_credit')
        self.df_dl_customer_360 = session.table('s3lakehouse.data_product_customer_360.customer_information')

        self.df_onprem_credit = self.df_onprem_credit.with_column('risk_appetite', f.sql_expr("replace(\"risk_appetite\", 'wild_west', 'very_low')"))
        self.df_joined = self.df_onprem_credit.join(self.df_dl_customer_360, self.df_onprem_credit['custkey'] == self.df_dl_customer_360['custkey'])

        self.segments = self.df_onprem_credit.select('customer_segment').distinct().to_pandas()['customer_segment'].to_list()

        self.query_history = session.query_history()
        self.queries_list = list()

    def get_queries(self) -> list:
        return self.query_history.queries

    def get_unique_segs(self) -> pd.DataFrame:
        return self.segments

    def get_agg_data(self, segment) -> pd.DataFrame:
        # Drive the aggergate data

        if segment != '':
            df_summary = self.df_joined.filter(col('customer_segment') == segment)\
            .group_by('state')\
            .count()\
            .sort(col('count'), ascending=False)
        else:
            df_summary = self.df_joined\
            .group_by('state')\
            .count()\
            .sort(col('count').desc())

        return df_summary.to_pandas()

with gr.Blocks() as demo:
    gr.Markdown(
    """
    # A simple demo showing a data app driven by PyStarburst.
    """)
    
    my_data = Data()

    segs = my_data.get_unique_segs()

    with gr.Row():
        seg = gr.Dropdown(segs, label='Segment', value='silver', allow_custom_value=False)
 
    plt = gr.BarPlot(x='state', y='count', tooltip=['state','count'], y_title='Num of Customers', x_title='State')
    
    queries = gr.Dataframe([], type='array', interactive=False, wrap=True,
                           headers=['QueryID', 'QueryText'], datatype=['str', 'str'], label='Query History')

    btn = gr.Button('Refresh Query History')
    btn.click(my_data.get_queries, [], queries, queue=False)
    
    plt.change(my_data.get_queries, [], queries, queue=False)
    seg.change(my_data.get_agg_data, [seg], plt, queue=False)
    demo.load(my_data.get_agg_data, [seg], plt, queue=False)

demo.queue().launch(server_name="0.0.0.0")