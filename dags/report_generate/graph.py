import pandas as pd
import plotly.express as px
import numpy as np
from datetime import datetime
import plotly.io as pio
import os
import plotly.graph_objects as go

from report_generate.helpers import get_data_from_api, get_metadata

# Define a function to calculate the percentage change
def calculate_percentage_change(group):
    if len(group) < 2:
        return pd.Series([None], index=['percentage_change'])
    
    # Check if Monday's data is missing, then use Tuesday's data
    if group.iloc[0]['timestamp'].dayofweek == 0 and len(group) == 1:
        open_price = group.iloc[1]['open']
    else:
        open_price = group.iloc[0]['open']
    
    # Check if Friday's data is missing, then use Thursday's data
    if group.iloc[-1]['timestamp'].dayofweek == 4 and len(group) == 1:
        close_price = group.iloc[-2]['close']
    else:
        close_price = group.iloc[-1]['close']
    
    # Calculate the percentage change based on the criteria
    percentage_change = ((close_price - open_price) / open_price) * 100
    
    return pd.Series([percentage_change], index=['percentage_change'])


def plot_weekly_percentage_change():
    df = get_data_from_api()
    metadata = get_metadata()

    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df.sort_values(by=['sym', 'timestamp'], inplace=True)
    current_week = datetime.now().isocalendar()[1]
    previous_week = current_week - 1
    print(previous_week)

    previous_week_data = df[df['timestamp'].dt.isocalendar().week == previous_week]

    percentage_change_df = previous_week_data.groupby('sym').apply(calculate_percentage_change)

    # Add colour column based on percentage_change
    percentage_change_df['colour'] = 'green'
    percentage_change_df.loc[percentage_change_df['percentage_change'] < 0, 'colour'] = 'red'

    percentage_change_df['absolute_percentage'] = percentage_change_df['percentage_change'].abs()

    replace_dict = {v: k for k, v in metadata.items()}
    percentage_change_df = percentage_change_df.reset_index()
    percentage_change_df['sym'] = percentage_change_df['sym'].str[:-2]
    percentage_change_df['sym2'] = percentage_change_df['sym'].replace(replace_dict)

    max_value = percentage_change_df['absolute_percentage'].max()

    # Sort the DataFrame by absolute_percentage
    percentage_change_df_sorted = percentage_change_df.sort_values(by='percentage_change', ascending=True)

    # Create a Plotly bar chart
    fig = go.Figure()

    fig.add_trace(go.Bar(
        y=percentage_change_df_sorted.sym2,
        x=percentage_change_df_sorted['absolute_percentage'],
        orientation='h',
        marker=dict(
            color=percentage_change_df_sorted['colour']
        ),
        text=percentage_change_df_sorted['absolute_percentage'].round(2).astype(str) + '%',  # Define text to be displayed at each bar
        textposition='outside',  # Position the text outside the bars
        textfont=dict(color='black')  # Set text color
    ))

    # Customize layout
    fig.update_layout(
        title='Percentage Change of Stocks in Last Week',
        xaxis_title='Absolute Percentage Change (%)',
        yaxis_title='Stock Symbol',
        template='plotly_white',
        height=len(percentage_change_df_sorted.index) * 30,
        xaxis=dict(range=[0, max_value+10])
    )

    return fig