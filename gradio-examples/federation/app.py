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

session = Session.builder.configs(session_properties).create()

import time

def to_pandas_df(pystarburst_df):
    print("starting collect dataframe")
    start = time.time()
    
    sb_df = pystarburst_df.collect()
    
    end = time.time()
    print(f'Operation took {end-start}')

    print("starting convert to pandas")
    start = time.time()
    
    pd_df = pd.DataFrame(sb_df)
    
    end = time.time()
    print(f'Operation took {end-start}')

    return pd_df

def get_unique_segs():
    print("starting build segs dataframe")
    start = time.time()
    
    df_sb = session.table('sep_dataproducts.customer_360.customer_with_credit')
    segments = df_sb.select('customer_segment').distinct()
    
    end = time.time()
    print(f'Operation took {end-start}')
    
    return to_pandas_df(segments)['customer_segment'].to_list()

def get_agg_data(segment='ALL'):
    print("starting build agg dataframe")
    start = time.time()
    
    df_dl_customer_360 = session.table("s3lakehouse.data_product_customer_360.customer_information")
    df_onprem_credit = session.table("sep_dataproducts.customer_360.customer_with_credit")

    df_onprem_credit = df_onprem_credit.with_column("risk_appetite", f.sql_expr("replace(\"risk_appetite\", 'wild_west', 'very_low')"))
    df_joined = df_onprem_credit.join(df_dl_customer_360, df_onprem_credit['custkey'] == df_dl_customer_360['custkey'])

    if segment != 'ALL':
        df_joined = df_joined.filter(col('customer_segment') == segment)

    df_summary = df_joined\
    .group_by("state")\
    .count()\
    .sort(col("count"), ascending=False)
    
    end = time.time()
    print(f'Operation took {end-start}')

    return to_pandas_df(df_summary)

with gr.Blocks() as demo:
    gr.Markdown(
    """
    # A simple demo showing a data app driven by PyStarburst.
    """)
    import time
    
    segs = get_unique_segs()

    with gr.Row():
        seg = gr.Dropdown(segs, label="Segment", value="ALL", allow_custom_value=True)
 
    #data = get_agg_data()

    plt = gr.BarPlot(x='state', y='count', tooltip=['state','count'], y_title="Num of Customers", x_title="State")

    seg.change(get_agg_data, [seg], plt, queue=False)
    demo.load(get_agg_data, [seg], plt, queue=False)    

demo.launch(server_name="0.0.0.0")