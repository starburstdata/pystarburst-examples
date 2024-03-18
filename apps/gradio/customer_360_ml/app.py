'''
Copyright 2024 Starburst Data

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

# Dependencies
import gradio as gr

import pandas as pd
pd.options.plotting.backend = 'plotly'

# Settings
import env

# Data and ML Wrappers
from dataModels import Data
from mlModels import OpenAI

ML_MODELS = ['OpenAI ChatGPT']

# Settings
sb_host = env.HOST
sb_user = env.USERNAME
open_ai_key = env.OPENAI_API_KEY
open_ai_model = env.OPENAI_MODEL

def main():
    '''Main function to run the app - a demo of Galaxy, Gradio, and ChatGPT working together to analyze customer data.'''
    
    # Load Local Modules
    my_data = Data()
    
    if env.ENABLE_OPENAI: my_model = OpenAI(my_data)

    # Main Tab for the demo
    with gr.Blocks() as demo_tab:
        def save_seetings_ev(sb_txt_host: str, sb_txt_user: str, open_ai_key: str = None, open_ai_model: str = None,):
            '''UI Event Handler to save settings'''
            sb_host = sb_txt_host
            sb_user = sb_txt_user

            try:
                my_data.save_settings(sb_host, sb_user)

                my_model.save_settings(open_ai_key, open_ai_model)
                gr.Info('Settings Saved')
            except Exception as e:
                gr.Error(f'Error saving settings: {e}')  

        
        gr.Markdown(
        """
        ![Starburst](https://starbursttelemetry.galaxy.starburst.io/images/galaxy-logo.svg)
        
        # 
        # A data app demonstrating customer analysis using PyStarburst and an ML Model
        ## Source code is available on [Github](https://github.com/starburstdata/pystarburst-examples)
        """)

        # We've optionally added the code snippets to the UI
        if env.SHOW_SAMPLE_CODE:
            gr.Code(my_data.INITIAL_CODE, language='python', label='Initial Code', interactive=False)
            gr.Code(my_data.LOAD_CODE, language='python', label='Load Data', interactive=False)

        # Filters
        gr.Markdown('# Filters')

        # More snippets
        if env.SHOW_SAMPLE_CODE:
            gr.Markdown('First load the data frames')
            gr.Code(my_data.SEGMENTS_CODE, language='python', label='Unique Segments', interactive=False, lines=2)

        seg = gr.Dropdown(my_data.segments, label='Segment', value=my_data.segments, allow_custom_value=True, multiselect=True)
    
        # Graphs
        gr.Markdown('# Customer Segments by State')

        # More snippets
        if env.SHOW_SAMPLE_CODE:
            gr.Code(my_data.AGG_CODE, language='python', label='Aggregate Data', interactive=False)

        plt = gr.BarPlot(x='state', y='count', tooltip=['state','count'], 
                         color='risk_appetite', y_title='Num of Customers', x_title='State')

        def load_dropdown():
            '''UI Event Handler to load dropdowns for segments.'''

            # Pull the data using our wrapper class and Python Starburst
            my_data.get_initial_data()

            return gr.Dropdown(choices=my_data.segments, value = my_data.segments, label='Segment', allow_custom_value=True)
        
        # Now some Gen AI
        if env.ENABLE_OPENAI:
            gr.Markdown('# Ask a question in naturual language on the data')

            model = gr.Dropdown(ML_MODELS, label='Model Type', value='OpenAI ChatGPT', allow_custom_value=False)
            
            question = gr.Textbox('Which 5 states have the highest risk appetite? Why?', label='Question')
            btn_ask = gr.Button('Ask')         

            response = gr.TextArea('', label='Response', lines=5, interactive=False)

            btn_ask.click(fn=my_model.predict,
                    inputs=[question],
                    outputs=[response], queue=True)

        if env.ENABLE_WRITE:
            gr.Markdown('## Optional: Write Data')
            if env.SHOW_SAMPLE_CODE:
                gr.Code(my_data.WRITE_CODE, language='python', label='Load Data', interactive=False)

            btn_write = gr.Button('Write Data')
            summary = gr.Dataframe([], type='array', interactive=False, wrap=True,
                                headers=['State', 'Risk_Appetite', 'Count_of_Customers'], datatype=['str', 'str', 'number'], label='Output Table')
        
        # Event Handlers
        if env.ENABLE_WRITE: btn_write.click(my_data.write_agg_data, [], [summary], queue=True)
        demo_tab.load(load_dropdown, [], [], queue=True)
        demo_tab.load(my_data.get_agg_data, [seg], [plt], queue=True)
        seg.change(my_data.get_agg_data, [seg], plt, queue=True)

    # Query History tab showing queries issues to Galaxy
    with gr.Blocks() as query_tab:
            gr.Markdown('Query History')
            queries = gr.Dataframe([], type='array', interactive=False, wrap=True,
                                headers=['QueryID', 'QueryText'], datatype=['str', 'str'], label='Query History')

            btn = gr.Button('Refresh Query History')
            btn.click(my_data.get_queries, [], queries, queue=True, every=20)

    # Settings Tab
    with gr.Blocks() as settings_tab:
        gr.Markdown('# Note: These settings are for the session only. You can preset them in the env.py and .env files.')

        gr.Markdown('Starburst Settings')
        sb_txt_host = gr.Textbox(my_data.host, label='Starburst Host')
        sb_txt_user = gr.Textbox(my_data.username, label='Username/role')

        if env.ENABLE_OPENAI:
            gr.Markdown('ML Model Settings')
            open_ai_key = gr.Textbox(env.OPENAI_API_KEY, label='API Key', type='password')
            open_ai_model = gr.Dropdown(my_model.models, label='Model', value=env.OPENAI_MODEL, allow_custom_value=True)

        save = gr.Button('Save Settings')
        if env.ENABLE_OPENAI:
            save.click(save_seetings_ev, inputs=[sb_txt_host, sb_txt_user, open_ai_key, open_ai_model], queue=True)
        else:
            save.click(save_seetings_ev, inputs=[sb_txt_host, sb_txt_user], queue=True)

    # Main Loaders
    if env.SHOW_SETTINGS:
        demo = gr.TabbedInterface([demo_tab, settings_tab, query_tab], ['Demo', 'Settings', 'Query History'], analytics_enabled=True).queue()
    else:
        demo = gr.TabbedInterface([demo_tab, query_tab], ['Demo', 'Query History'], analytics_enabled=True).queue()
    demo.launch(server_name=env.BIND_HOST, server_port=env.PORT, share=env.SHARE, debug=env.DEBUG)


if __name__ == '__main__': main()
