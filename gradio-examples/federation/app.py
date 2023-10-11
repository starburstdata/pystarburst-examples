import gradio as gr
from datetime import date, datetime
from dateutil.relativedelta import relativedelta
import pandas as pd

pd.options.plotting.backend = "plotly"

from pystarburst import Session
from pystarburst import functions as f
from pystarburst.functions import col

import trino

import env

class Data():
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
        session = Session.builder.configs(self.session_properties).create()

        self.df_onprem_credit = session.table('sep_dataproducts.customer_360.customer_with_credit')
        self.df_dl_customer_360 = session.table('s3lakehouse.data_product_customer_360.customer_information')

        self.df_onprem_credit = self.df_onprem_credit.with_column('risk_appetite', f.sql_expr("replace(\"risk_appetite\", 'wild_west', 'very_low')"))
        self.df_joined = self.df_onprem_credit.join(self.df_dl_customer_360, self.df_onprem_credit['custkey'] == self.df_dl_customer_360['custkey'])

        self.segments = self.df_onprem_credit.select('customer_segment').distinct().to_pandas()['customer_segment'].to_list()

        self.query_history = session.query_history()
        self.queries_list = list()

    def refresh_session(self):
        session = Session.builder.configs(self.session_properties).create()

    def save_settings(self, h, u, p):
        self.host = h
        self.username = u
        self.password = p
        self.refresh_session()

    def get_queries(self) -> list:
        return self.query_history.queries

    def get_unique_segs(self) -> pd.DataFrame:
        return self.segments

    def get_agg_data(self, segment) -> pd.DataFrame:
        # Drive the aggergate data

        if segment != '':
            df_summary = self.df_joined.filter(col('customer_segment') == segment)\
            .group_by('state', 'risk_appetite')\
            .count()\
            .sort(col('count').desc())
        else:
            df_summary = self.df_joined\
            .group_by('state')\
            .count()\
            .sort(col('count').desc())

        return df_summary.to_pandas()

with gr.Blocks() as demo_tab:
    gr.Markdown(
    """
    # A simple demo showing a data app driven by PyStarburst.
    """)
    
    my_data = Data()

    with gr.Row():
        seg = gr.Dropdown(my_data.segments, label='Segment', value='silver', allow_custom_value=False)
 
    plt = gr.BarPlot(x='state', y='count', tooltip=['state','count'], color='risk_appetite', y_title='Num of Customers', x_title='State')

    seg.change(my_data.get_agg_data, [seg], plt, queue=False)
    demo_tab.load(my_data.get_agg_data, [seg], plt, queue=False)

with gr.Blocks() as settings_tab:
    gr.Markdown("Note: These settings are for the session only. You can preset them in the env.py file.")
    txt_host = gr.Textbox(my_data.host, label="Starburst Host")
    txt_user = gr.Textbox(my_data.username, label="Username")
    txt_pass = gr.Textbox(my_data.password, label="Password", type="password")
    save = gr.Button('Save Settings')
    save.click(my_data.save_settings, inputs=[txt_host, txt_user, txt_pass])

    queries = gr.Dataframe([], type='array', interactive=False, wrap=True,
                           headers=['QueryID', 'QueryText'], datatype=['str', 'str'], label='Query History')

    btn = gr.Button('Refresh Query History')
    btn.click(my_data.get_queries, [], queries, queue=False)

demo = gr.TabbedInterface([demo_tab, settings_tab], ['Demo', 'Settings'])

demo.queue().launch(server_name="0.0.0.0")