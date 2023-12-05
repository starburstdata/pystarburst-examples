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


import gradio as gr

import pandas as pd
pd.options.plotting.backend = "plotly"

from dotenv import load_dotenv
load_dotenv("./.env")
import env

from dataModels import Data
from mlModels import OpenAI

ML_MODELS = ['OpenAI ChatGPT']

# Settings
sb_host = env.HOST
sb_user = env.USERNAME
sb_pass = env.PASSWORD
open_ai_key = env.OPENAI_API_KEY
open_ai_model = env.OPENAI_MODEL

def save_seetings_ev(self, sb_txt_host, sb_txt_user, sb_txt_pass, data_class: Data, open_ai_key = None, open_ai_model = None, openai_class: OpenAI = None):
    sb_host = sb_txt_host
    sb_user = sb_txt_user
    sb_pass = sb_txt_pass

    data_class.save_settings(sb_host, sb_user, sb_pass)

    if openai_class is not None:
        open_ai_key = open_ai_model
        open_ai_model = open_ai_model

        openai_class.save_settings(open_ai_key, open_ai_model)

def main():
    # A gradio app to demo a customer 360 ML app
    
    # Load Local Modules
    my_data = Data()
    #my_data.get_initial_data()

    my_model = OpenAI(my_data)

    # Main Tab for the demo
    with gr.Blocks() as demo_tab:
        gr.Markdown(
        """
        ![Starburst](https://starbursttelemetry.galaxy.starburst.io/images/galaxy-logo.svg)
        
        # 
        # A quick customer analysis using PyStarburst and an ML Model
        ## Source code is available on [Github](https://github.com/starburstdata/pystarburst-examples)
        
        First load the data frames
        """)

        # Filters
        seg = gr.Dropdown(my_data.segments, label='Segment', value=my_data.segments, allow_custom_value=True, multiselect=True)
        ris = gr.Dropdown(choices=my_data.risk_appetites, value = my_data.risk_appetites, label='Risk Appetite', allow_custom_value=True, multiselect=True)
        sta = gr.Dropdown(choices=my_data.states, value = my_data.states, label='State', allow_custom_value=True, multiselect=True)

        # Graphs & Tables
        gr.Markdown('# Number of Customer by Risk Appetite and State')
        plt = gr.BarPlot(x='state', y='count', tooltip=['state','count'], 
                         color='risk_appetite', y_title='Num of Customers', x_title='State')
        
        gr.Markdown('## Details')
        d = gr.DataFrame([], interactive=False, wrap=True)


        # Now some Gen AI
        gr.Markdown('Ask a question in naturual language on the data')

        model = gr.Dropdown(ML_MODELS, label='Model Type', value='OpenAI ChatGPT', allow_custom_value=False)
        
        question = gr.Textbox('Which 5 states have the highest risk appetite? Why?', label='Question')
        btn_ask = gr.Button('Ask')         

        response = gr.TextArea('', label='Response', lines=5, interactive=False)

        demo_tab.load(my_data.get_agg_data, [seg, ris, sta], [plt], queue=True)
        demo_tab.load(my_data.filter_accounts, [seg, ris, sta], [d], queue=True)

        btn_ask.click(fn=my_model.predict,
                inputs=[question],
                outputs=[response], queue=True)

        seg.change(my_data.get_agg_data, [seg, ris, sta], plt, queue=True)
        ris.change(my_data.get_agg_data, [seg, ris, sta], plt, queue=True)
        sta.change(my_data.get_agg_data, [seg, ris, sta], plt, queue=True)

        seg.change(my_data.filter_accounts, [seg, ris, sta], d, queue=True)
        ris.change(my_data.filter_accounts, [seg, ris, sta], d, queue=True)
        sta.change(my_data.filter_accounts, [seg, ris, sta], d, queue=True)

    # Query History
    with gr.Blocks() as query_tab:
            gr.Markdown('Query History')
            queries = gr.Dataframe([], type='array', interactive=False, wrap=True,
                                headers=['QueryID', 'QueryText'], datatype=['str', 'str'], label='Query History')

            btn = gr.Button('Refresh Query History')
            btn.click(my_data.get_queries, [], queries, queue=True, every=20)

    demo = gr.TabbedInterface([demo_tab, query_tab], ['Demo', 'Query History'], analytics_enabled=True).queue()
    demo.launch(server_name=env.BIND_HOST, server_port=env.PORT, share=env.SHARE, debug=env.DEBUG)


if __name__ == '__main__': main()
