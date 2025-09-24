from plotly.subplots import make_subplots
import plotly.graph_objects as go
import pandas as pd

def plot_price_trends(df, crypto_name):
    """
    Plots the price trends of a given cryptocurrency over time,
    with traded volume stacked as bars over time.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'minute', 'avg_price', and 'volume' columns.
    crypto_name (str): Name of the cryptocurrency for title.

    Returns:
    fig: Plotly figure object.
    """
    min_price, max_price = df['avg_price'].min(), df['avg_price'].max()
    range_padding = (max_price - min_price) * 0.3

    min_vol, max_vol = df['total_volume'].min(), df['total_volume'].max()
    vol_padding = (max_vol - min_vol) * 3

    # Create subplot with secondary y-axis
    fig = make_subplots(specs=[[{"secondary_y": True}]])

    fig.add_trace(
        go.Scatter(
            x=df['minute'],
            y=df['avg_price'],
            name='Avg Price',
            mode='lines',
            line = dict(color='lightblue')
        ),
        secondary_y=False,
    )

    # Add volume trace
    fig.add_trace(
        go.Scatter(
            x=df['minute'],
            y=df['total_volume'],
            name='Volume',
            mode='lines',
            line=dict(color='lightblue'),
            fill = 'tozeroy',
            fillcolor = 'lightblue',
            opacity=0.5
        ),
        secondary_y=True,
    )

    fig.update_layout(
        title=f'{crypto_name} Avg Price & Volume Traded',
        xaxis_title='Time',
        yaxis_title='Price (USD)',
        yaxis2_title='Volume Traded',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    fig.update_yaxes(
        range=[min_vol, max_vol + vol_padding],
        showgrid=False,
        secondary_y=True
    )
    fig.update_yaxes(
        range=[min_price - range_padding, max_price + range_padding],
        secondary_y=False)

    return fig


def plot_candlestick_chart(df, crypto_name):
    """
    Plots 5-minute candlestick chart for a given cryptocurrency.

    Parameters:
    df (pd.DataFrame): DataFrame containing 'minute', 'open_price', 'close_price',
                      'max_high', 'min_low', and 'total_volume' columns.
    crypto_name (str): Name of the cryptocurrency for title.

    Returns:
    fig: Plotly figure object.
    """
    # Create subplot with secondary y-axis for volume
    fig = make_subplots(
        rows=2, cols=1,
        row_heights=[0.7, 0.3],
        vertical_spacing=0.05,
        subplot_titles=[f'{crypto_name} Candlestick Chart', 'Volume'],
        specs=[[{"secondary_y": False}],
               [{"secondary_y": False}]]
    )

    # Add candlestick trace using your actual column names
    fig.add_trace(
        go.Candlestick(
            x=df['minute'],
            open=df['open_price'],
            high=df['high_price'],  # Using max_high from your data
            low=df['low_price'],    # Using min_low from your data
            close=df['close_price'],
            name='OHLC',
            increasing_line_color='green',
            decreasing_line_color='red'
        ),
        row=1, col=1
    )

    # Add volume bars using total_volume
    fig.add_trace(
        go.Bar(
            x=df['minute'],
            y=df['total_volume'],  # Using total_volume from your data
            name='Volume',
            marker_color='lightblue',
            opacity=0.6
        ),
        row=2, col=1
    )

    # Update layout
    fig.update_layout(
        title=f'{crypto_name} 5-Minute Candlestick Chart',
        xaxis_rangeslider_visible=False,
        height=600,
        showlegend=True
    )

    # Update x-axis labels
    fig.update_xaxes(title_text="Time", row=2, col=1)
    fig.update_yaxes(title_text="Price (USD)", row=1, col=1)
    fig.update_yaxes(title_text="Volume", row=2, col=1)

    return fig


def plot_sentiment_distribution(df, sentiment_type):
    """
    Plots sentiment distribution pie chart for FinBERT predictions.

    Parameters:
    df (pd.DataFrame): DataFrame containing sentiment data

    Returns:
    fig: Plotly figure object
    """
    if df.empty or sentiment_type not in df.columns:
        return None
    df = df.dropna(subset=[sentiment_type])

    sentiment_counts = df[sentiment_type].value_counts()

    colors = {
        'positive': '#2E8B57',
        'negative': '#DC143C',
        'neutral': '#808080'
    } if sentiment_type == 'finbert_sentiment' else {
        'bearish': '#D62728',
        'somewhat-bearish':'#EF8787',
        'neutral': '#808080',
        'somewhat-bullish': '#90D390',
        'bullish': '#2CA02C'
    }

    fig = go.Figure(data=[go.Pie(
        labels=sentiment_counts.index,
        values=sentiment_counts.values,
        marker=dict(colors=[colors.get(label.lower(), '#808080') for label in sentiment_counts.index]),
        textinfo='label+percent+value'
    )])

    fig.update_layout(
        title=f"News Sentiment Distribution ({sentiment_type})",
        height=400
    )

    return fig

def plot_sentiment_timeline(df, sentiment_type):
    """
    Plots sentiment over time with confidence scores.

    Parameters:
    df (pd.DataFrame): DataFrame containing news data with timestamps and sentiment

    Returns:
    fig: Plotly figure object
    """
    if df.empty:
        return None

    # Create color mapping for sentiment
    color_map = {
        'positive': 'green',
        'negative': 'red',
        'neutral': 'gray'
    } if sentiment_type == 'finbert_sentiment' else {
        'bullish': '#2CA02C',
        'somewhat-bullish': '#90D390',
        'neutral': '#808080',
        'somewhat-bearish': '#EF8787',
        'bearish': '#D62728'
    }

    fig = go.Figure()

    # Add scatter plot for each sentiment type
    for sentiment in df[sentiment_type].unique():
        if pd.notna(sentiment):
            sentiment_data = df[df[sentiment_type] == sentiment]
            fig.add_trace(go.Scatter(
                x=sentiment_data['published_at'],
                y=sentiment_data['finbert_score'] if sentiment_type == 'finbert_sentiment' else sentiment_data['alphavantage_sentiment_score'],
                mode='markers',
                name=sentiment.title(),
                marker=dict(
                    color=color_map.get(sentiment.lower(), 'gray'),
                    size=8,
                    opacity=0.7
                ),
                text=sentiment_data['title'],
                hovertemplate='<b>%{text}</b><br>Score: %{y:.3f}<br>Time: %{x}<extra></extra>'
            ))

    fig.update_layout(
        title=f"News Sentiment Timeline ({sentiment_type})",
        xaxis_title="Time",
        yaxis_title="Sentiment Confidence Score" if sentiment_type == 'finbert_sentiment' else "Sentiment Score",
        height=400,
        hovermode='closest'
    )

    return fig

def plot_sentiment_by_symbol(df, sentiment_type):
    """
    Plots average sentiment scores by symbol.

    Parameters:
    df (pd.DataFrame): DataFrame containing news data

    Returns:
    fig: Plotly figure object
    """
    if df.empty:
        return None
    df = df.dropna(subset=[sentiment_type])

    # Calculate average sentiment scores by symbol
    sentiment_summary = df.groupby(['symbol', sentiment_type]).size().unstack(fill_value=0)

    fig = go.Figure()

    colors = ['#2E8B57', '#808080', '#DC143C'] if sentiment_type == 'finbert_sentiment' else ['#2CA02C', '#90D390', '#808080', '#EF8787', '#D62728']
    sentiments = df[sentiment_type].unique()

    for i, sentiment in enumerate(sentiments):
        if sentiment in sentiment_summary.columns:
            fig.add_trace(go.Bar(
                name=sentiment.title(),
                x=sentiment_summary.index,
                y=sentiment_summary[sentiment],
                marker_color=colors[i]
            ))

    fig.update_layout(
        title="News Sentiment Distribution by Symbol",
        xaxis_title="Ticker Symbol",
        yaxis_title="Number of News Articles",
        barmode='stack',
        height=400
    )

    return fig

def create_news_summary_cards(df):
    """
    Creates summary statistics for news data.

    Parameters:
    df (pd.DataFrame): DataFrame containing news data

    Returns:
    dict: Summary statistics
    """
    if df.empty:
        return {
            'total_articles': 0,
            'avg_alphavantage_score': 0,
            'most_common_alphavantage_sentiment': 'N/A',
            'most_commmon_finbert_sentiment': 'N/A',
            'latest_article_time': 'N/A'
        }

    return {
        'total_articles': len(df),
        'avg_alphavantage_score': df['alphavantage_sentiment_score'].mean() if 'alphavantage_sentiment_score' in df.columns else 0,
        'most_common_alphavantage_sentiment': df['alphavantage_sentiment'].mode().iloc[0] if not df['alphavantage_sentiment'].mode().empty else 'N/A',
        'most_commmon_finbert_sentiment': df['finbert_sentiment'].mode().iloc[0] if not df['finbert_sentiment'].mode().empty else 'N/A',
        'latest_article_time': df['published_at'].max() if 'published_at' in df.columns else 'N/A'
    }
