# streamlit_app.py

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
import numpy as np

# Page configuration
st.set_page_config(
    page_title="Aviation Grievances (India) Analytics Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-container {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        margin: 0.5rem 0;
    }
    .stSelectbox > div > div > select {
        background-color: #ffffff;
    }
</style>
""", unsafe_allow_html=True)

# Initialize BigQuery client
@st.cache_resource
def init_bigquery_client():
    credentials = service_account.Credentials.from_service_account_info(
        st.secrets["gcp_service_account"]
    )
    return bigquery.Client(credentials=credentials)

client = init_bigquery_client()

# Cache data queries with TTL
@st.cache_data(ttl=600)
def run_query(query):
    """Execute BigQuery with caching"""
    try:
        query_job = client.query(query)
        rows_raw = query_job.result()
        rows = [dict(row) for row in rows_raw]
        return rows
    except Exception as e:
        st.error(f"Query failed: {str(e)}")
        return []

# Get available date range from materialized view
@st.cache_data(ttl=3600)
def get_date_range():
    """Get min and max dates from the materialized view"""
    query = """
    SELECT 
        MIN(Date_Inserted) as min_date,
        MAX(Date_Inserted) as max_date
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_daily_aggregation`
    """
    result = run_query(query)
    if result:
        return result[0]['min_date'], result[0]['max_date']
    return None, None

# Main data query function using materialized view
def get_grievance_data(start_date, end_date, airlines=None):
    """Fetch grievance data from materialized view with filtering"""
    
    # Add airline filter if specified
    airline_filter = ""
    if airlines and len(airlines) > 0:
        airline_list = "', '".join(airlines)
        airline_filter = f"AND Airline IN ('{airline_list}')"
    
    query = f"""
    SELECT 
        Date_Inserted,
        Airline,
        Type,
        Total_Received,
        Active_Grievances_Without_Escalation,
        Active_Grievances_With_Escalation,
        Closed_Grievances_Without_Escalation,
        Closed_Grievances_With_Escalation,
        Grievances_Without_Ratings,
        Grievances_With_Ratings,
        Grievances_With_Very_Good_Rating,
        Grievances_With_Good_Rating,
        Grievances_With_OK_Rating,
        Grievances_With_Bad_Rating,
        Grievances_With_Very_Bad_Rating,
        Twitter_Grievances,
        Facebook_Grievances,
        Grievances_Additional_Info_Provided,
        Grievances_Additional_Info_Not_Provided,
        Grievances_Without_Feedback,
        Grievances_With_Feedback,
        Grievances_With_Feedback_Issue_Not_Resolved,
        Grievances_With_Feedback_Issue_Resolved    
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_daily_aggregation`
    WHERE Date_Inserted >= '{start_date}'
    AND Date_Inserted <= '{end_date}'
    {airline_filter}
    ORDER BY Date_Inserted DESC
    """
    return run_query(query)

# Get unique airline companies from materialized view
@st.cache_data(ttl=3600)
def get_airline_companies():
    """Get unique airline companies for filtering"""
    query = """
    SELECT DISTINCT Airline
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_daily_aggregation`
    WHERE Airline IS NOT NULL
    ORDER BY Airline
    """
    result = run_query(query)
    return [row['Airline'] for row in result]

# Get unique types from materialized view
@st.cache_data(ttl=3600)
def get_grievance_types():
    """Get unique grievance types for filtering"""
    query = """
    SELECT DISTINCT Type
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_daily_aggregation`
    WHERE Type IS NOT NULL
    ORDER BY Type
    """
    result = run_query(query)
    return [row['Type'] for row in result]

def calculate_metrics(df):
    """Calculate derived metrics from the available columns"""
    # Calculate additional derived metrics
    df['Total_Active'] = df['Active_Grievances_Without_Escalation'] + df['Active_Grievances_With_Escalation']
    df['Total_Closed'] = df['Closed_Grievances_Without_Escalation'] + df['Closed_Grievances_With_Escalation']
    df['Total_Escalated'] = df['Active_Grievances_With_Escalation'] + df['Closed_Grievances_With_Escalation']
    df['Total_Social_Media'] = df['Twitter_Grievances'] + df['Facebook_Grievances']
    
    # Calculate rates (handle division by zero)
    df['Resolution_Rate_Percent'] = np.where(
        df['Total_Received'] > 0,
        (df['Total_Closed'] / df['Total_Received']) * 100,
        0
    )
    
    df['Escalation_Rate_Percent'] = np.where(
        df['Total_Received'] > 0,
        (df['Total_Escalated'] / df['Total_Received']) * 100,
        0
    )
    
    df['Social_Media_Rate_Percent'] = np.where(
        df['Total_Received'] > 0,
        (df['Total_Social_Media'] / df['Total_Received']) * 100,
        0
    )
    
    df['Feedback_Response_Rate_Percent'] = np.where(
        df['Total_Received'] > 0,
        (df['Grievances_With_Feedback'] / df['Total_Received']) * 100,
        0
    )
    
    # Calculate satisfaction score (weighted average of ratings)
    total_rated = (df['Grievances_With_Very_Good_Rating'] + 
                   df['Grievances_With_Good_Rating'] + 
                   df['Grievances_With_OK_Rating'] + 
                   df['Grievances_With_Bad_Rating'] + 
                   df['Grievances_With_Very_Bad_Rating'])
    
    df['Satisfaction_Score'] = np.where(
        total_rated > 0,
        (df['Grievances_With_Very_Good_Rating'] * 5 + 
         df['Grievances_With_Good_Rating'] * 4 + 
         df['Grievances_With_OK_Rating'] * 3 + 
         df['Grievances_With_Bad_Rating'] * 2 + 
         df['Grievances_With_Very_Bad_Rating'] * 1) / total_rated,
        0
    )
    
    return df

# Dashboard Header
st.markdown('<h1 class="main-header">✈️ Airlines Grievance Analytics Dashboard</h1>', unsafe_allow_html=True)

# Sidebar for filters
st.sidebar.header("📋 Filters & Settings")

# Date range selector
min_date, max_date = get_date_range()
if min_date and max_date:
    col1, col2 = st.sidebar.columns(2)
    with col1:
        # Calculate default start date, ensuring it's within valid range
        default_start = max(min_date, max_date - timedelta(days=30))
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            min_value=min_date,
            max_value=max_date
        )
    with col2:
        end_date = st.date_input(
            "End Date",
            value=max_date,
            min_value=min_date,
            max_value=max_date
        )
else:
    st.sidebar.error("Unable to fetch date range from database")
    st.stop()

# Airline company filter
airline_companies = get_airline_companies()
selected_airlines = st.sidebar.multiselect(
    "Select Airlines",
    options=airline_companies,
    default=airline_companies[:5] if len(airline_companies) > 5 else airline_companies
)

# Auto-refresh toggle
auto_refresh = st.sidebar.checkbox("Auto-refresh (10 min)", value=True)

# Fetch data
if st.sidebar.button("🔄 Refresh Data") or auto_refresh:
    data = get_grievance_data(start_date, end_date, selected_airlines)
    
    if not data:
        st.warning("No data found for the selected criteria")
        st.stop()
    
    # Convert to DataFrame and calculate metrics
    df = pd.DataFrame(data)
    df['Date_Inserted'] = pd.to_datetime(df['Date_Inserted'])
    df = calculate_metrics(df)
    
    # Key Metrics Row
    st.markdown("## 📊 Key Performance Indicators")
    
    col1, col2, col3, col4, col5, col6 = st.columns(6)
    
    with col1:
        total_received = df['Total_Received'].sum()
        st.metric("Total Grievances", f"{total_received:,}")
    
    with col2:
        total_active = df['Total_Active'].sum()
        st.metric("Active Grievances", f"{total_active:,}")
    
    with col3:
        total_closed = df['Total_Closed'].sum()
        st.metric("Closed Grievances", f"{total_closed:,}")
    
    with col4:
        avg_resolution_rate = df['Resolution_Rate_Percent'].mean()
        st.metric("Avg Resolution Rate", f"{avg_resolution_rate:.1f}%")
    
    with col5:
        avg_escalation_rate = df['Escalation_Rate_Percent'].mean()
        st.metric("Avg Escalation Rate", f"{avg_escalation_rate:.1f}%")
    
    with col6:
        avg_satisfaction = df['Satisfaction_Score'].mean()
        st.metric("Avg Satisfaction", f"{avg_satisfaction:.1f}/5")
    
    # Create visualizations
    st.markdown("## 📈 Analytics & Insights")
    
    # Row 1: Time series and airline by type chart
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📅 Grievances Over Time")
        daily_stats = df.groupby('Date_Inserted').agg({
            'Total_Received': 'sum',
            'Total_Active': 'sum',
            'Total_Closed': 'sum'
        }).reset_index()
        
        fig_time = go.Figure()
        fig_time.add_trace(go.Scatter(
            x=daily_stats['Date_Inserted'],
            y=daily_stats['Total_Received'],
            name='Total Received',
            line=dict(color='#1f77b4', width=3)
        ))
        fig_time.add_trace(go.Scatter(
            x=daily_stats['Date_Inserted'],
            y=daily_stats['Total_Active'],
            name='Active',
            line=dict(color='#ff7f0e', width=2)
        ))
        fig_time.add_trace(go.Scatter(
            x=daily_stats['Date_Inserted'],
            y=daily_stats['Total_Closed'],
            name='Closed',
            line=dict(color='#2ca02c', width=2)
        ))
        
        fig_time.update_layout(
            xaxis_title="Date",
            yaxis_title="Number of Grievances",
            hovermode='x unified',
            height=400
        )
        st.plotly_chart(fig_time, use_container_width=True)
    
    with col2:
        st.markdown("### 🔄 Grievances by Airline & Type")
        airline_type_stats = df.groupby(['Airline', 'Type'])['Total_Received'].sum().reset_index()
        airline_type_stats = airline_type_stats.sort_values('Total_Received', ascending=False)
        
        fig_airline_type = px.bar(
            airline_type_stats,
            x='Airline',
            y='Total_Received',
            color='Type',
            title="Total Grievances by Airline and Type",
            labels={'Total_Received': 'Total Grievances', 'Airline': 'Airline'}
        )
        fig_airline_type.update_xaxes(tickangle=45)
        fig_airline_type.update_layout(height=400)
        st.plotly_chart(fig_airline_type, use_container_width=True)
    
    # Row 2: Airlines distribution and rating analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ✈️ Airlines Distribution")
        airline_stats = df.groupby('Airline')['Total_Received'].sum().reset_index()
        airline_stats = airline_stats.sort_values('Total_Received', ascending=False)
        
        fig_airline = px.pie(
            airline_stats,
            values='Total_Received',
            names='Airline',
            title="Total Grievances by Airline"
        )
        fig_airline.update_layout(height=400)
        st.plotly_chart(fig_airline, use_container_width=True)
    
    with col2:
        st.markdown("### ⭐ Rating Distribution")
        rating_cols = ['Grievances_With_Very_Good_Rating', 'Grievances_With_Good_Rating', 
                      'Grievances_With_OK_Rating', 'Grievances_With_Bad_Rating', 'Grievances_With_Very_Bad_Rating']
        rating_sums = [df[col].sum() for col in rating_cols]
        rating_labels = ['Very Good', 'Good', 'OK', 'Bad', 'Very Bad']
        
        fig_ratings = go.Figure(data=[
            go.Bar(
                x=rating_labels,
                y=rating_sums,
                marker_color=['#2ca02c', '#7fcc7f', '#ffff99', '#ff7f0e', '#d62728']
            )
        ])
        fig_ratings.update_layout(
            title="Grievance Ratings",
            xaxis_title="Rating",
            yaxis_title="Number of Grievances",
            height=400
        )
        st.plotly_chart(fig_ratings, use_container_width=True)
    
    # Row 3: Social media and KPI heatmap
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📱 Social Media Grievances")
        social_data = {
            'Platform': ['Twitter', 'Facebook'],
            'Grievances': [df['Twitter_Grievances'].sum(), df['Facebook_Grievances'].sum()]
        }
        social_df = pd.DataFrame(social_data)
        
        fig_social = px.bar(
            social_df,
            x='Platform',
            y='Grievances',
            color='Platform',
            title="Social Media Platform Distribution"
        )
        fig_social.update_layout(height=400)
        st.plotly_chart(fig_social, use_container_width=True)
    
    with col2:
        st.markdown("### 🎯 KPI Heatmap by Airline")
        # Create heatmap of KPIs by airline
        kpi_data = df.groupby('Airline').agg({
            'Resolution_Rate_Percent': 'mean',
            'Escalation_Rate_Percent': 'mean',
            'Satisfaction_Score': 'mean',
            'Feedback_Response_Rate_Percent': 'mean'
        }).reset_index()
        
        if not kpi_data.empty and len(kpi_data) > 0:
            fig_heatmap = px.imshow(
                kpi_data.set_index('Airline').T,
                labels=dict(x="Airline", y="KPI", color="Score"),
                title="KPI Performance Heatmap by Airline",
                color_continuous_scale="RdYlGn"
            )
            fig_heatmap.update_layout(height=400)
            st.plotly_chart(fig_heatmap, use_container_width=True)
        else:
            st.info("No data available for KPI heatmap")
    
    # Row 4: Resolution analysis and feedback analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔄 Airlines Resolution Analysis")
        resolution_data = df.groupby('Airline').agg({
            'Total_Received': 'sum',
            'Total_Closed': 'sum',
            'Resolution_Rate_Percent': 'mean'
        }).reset_index()
        
        fig_resolution = px.scatter(
            resolution_data,
            x='Total_Received',
            y='Resolution_Rate_Percent',
            size='Total_Closed',
            color='Airline',
            title="Resolution Rate vs Total Grievances by Airline",
            labels={'Total_Received': 'Total Received', 'Resolution_Rate_Percent': 'Resolution Rate (%)', 'Airline': 'Airline'}
        )
        fig_resolution.update_layout(height=400)
        st.plotly_chart(fig_resolution, use_container_width=True)
    
    with col2:
        st.markdown("### 💬 Feedback Analysis")
        feedback_resolved = df['Grievances_With_Feedback_Issue_Resolved'].sum()
        feedback_unresolved = df['Grievances_With_Feedback_Issue_Not_Resolved'].sum()
        
        if feedback_resolved > 0 or feedback_unresolved > 0:
            fig_feedback = go.Figure(data=[
                go.Pie(
                    labels=['Issue Resolved', 'Issue Not Resolved'],
                    values=[feedback_resolved, feedback_unresolved],
                    hole=0.4,
                    marker_colors=['#2ca02c', '#d62728']
                )
            ])
            fig_feedback.update_layout(
                title="Feedback Resolution Status",
                height=400
            )
            st.plotly_chart(fig_feedback, use_container_width=True)
        else:
            st.info("No feedback data available")
    
    # New Row: Type Analysis
    st.markdown("### 📊 Grievance Type Analysis")
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Type Distribution")
        type_stats = df.groupby('Type')['Total_Received'].sum().reset_index()
        type_stats = type_stats.sort_values('Total_Received', ascending=False)
        
        fig_type = px.bar(
            type_stats,
            x='Type',
            y='Total_Received',
            title="Total Grievances by Type",
            labels={'Total_Received': 'Total Grievances'}
        )
        fig_type.update_xaxes(tickangle=45)
        fig_type.update_layout(height=400)
        st.plotly_chart(fig_type, use_container_width=True)
    
    with col2:
        st.markdown("#### Type Performance Metrics")
        type_kpi = df.groupby('Type').agg({
            'Total_Received': 'sum',
            'Resolution_Rate_Percent': 'mean',
            'Escalation_Rate_Percent': 'mean',
            'Satisfaction_Score': 'mean'
        }).reset_index()
        
        fig_type_kpi = px.scatter(
            type_kpi,
            x='Resolution_Rate_Percent',
            y='Satisfaction_Score',
            size='Total_Received',
            color='Type',
            title="Type Performance: Resolution Rate vs Satisfaction",
            labels={'Resolution_Rate_Percent': 'Resolution Rate (%)', 'Satisfaction_Score': 'Satisfaction Score'}
        )
        fig_type_kpi.update_layout(height=400)
        st.plotly_chart(fig_type_kpi, use_container_width=True)
    
    # Detailed Data Table
    st.markdown("## 📋 Detailed Data")
    
    # Add filters for the table
    col1, col2, col3 = st.columns(3)
    with col1:
        show_only_active = st.checkbox("Show only active grievances")
    with col2:
        min_grievances = st.slider("Minimum grievances", 0, int(df['Total_Received'].max()), 0)
    with col3:
        selected_types = st.multiselect("Filter by Type", options=df['Type'].unique())
    
    # Filter data for table
    table_df = df.copy()
    if show_only_active:
        table_df = table_df[table_df['Total_Active'] > 0]
    if min_grievances > 0:
        table_df = table_df[table_df['Total_Received'] >= min_grievances]
    if selected_types:
        table_df = table_df[table_df['Type'].isin(selected_types)]
    
    # Select columns for display
    display_cols = ['Date_Inserted', 'Airline', 'Type', 'Total_Received', 
                   'Total_Active', 'Total_Closed', 'Resolution_Rate_Percent', 
                   'Escalation_Rate_Percent', 'Satisfaction_Score']
    st.dataframe(
        table_df[display_cols].sort_values('Date_Inserted', ascending=False),
        use_container_width=True,
        height=400
    )
    
    # Summary statistics
    st.markdown("## 📊 Summary Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Airlines Performance")
        airline_summary = df.groupby('Airline').agg({
            'Total_Received': ['sum', 'mean'],
            'Resolution_Rate_Percent': 'mean',
            'Escalation_Rate_Percent': 'mean',
            'Satisfaction_Score': 'mean'
        }).round(2)
        airline_summary.columns = ['Total', 'Daily Avg', 'Avg Resolution %', 'Avg Escalation %', 'Avg Satisfaction']
        st.dataframe(airline_summary)
    
    with col2:
        st.markdown("### Overall Metrics")
        overall_stats = {
            'Metric': ['Total Grievances', 'Average Daily Grievances', 'Peak Daily Grievances', 
                      'Overall Resolution Rate', 'Overall Escalation Rate', 'Overall Satisfaction',
                      'Social Media Rate', 'Feedback Response Rate'],
            'Value': [
                f"{df['Total_Received'].sum():,}",
                f"{df['Total_Received'].mean():.1f}",
                f"{df['Total_Received'].max():,}",
                f"{df['Resolution_Rate_Percent'].mean():.1f}%",
                f"{df['Escalation_Rate_Percent'].mean():.1f}%",
                f"{df['Satisfaction_Score'].mean():.1f}/5",
                f"{df['Social_Media_Rate_Percent'].mean():.1f}%",
                f"{df['Feedback_Response_Rate_Percent'].mean():.1f}%"
            ]
        }
        st.dataframe(pd.DataFrame(overall_stats), hide_index=True)
    
    # Export functionality
    st.markdown("## 💾 Export Data")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        csv_data = df.to_csv(index=False)
        st.download_button(
            label="📥 Download CSV",
            data=csv_data,
            file_name=f"grievance_data_{start_date}_{end_date}.csv",
            mime="text/csv"
        )
    
    with col2:
        # Summary report
        summary_report = f"""
# Airlines Grievance Analytics Report
## Period: {start_date} to {end_date}

### Key Metrics:
- Total Grievances: {total_received:,}
- Active Grievances: {total_active:,}
- Closed Grievances: {total_closed:,}
- Average Resolution Rate: {avg_resolution_rate:.1f}%
- Average Escalation Rate: {avg_escalation_rate:.1f}%
- Average Satisfaction Score: {avg_satisfaction:.1f}/5

### Top Airlines:
{airline_stats.head().to_string(index=False)}

### Performance Insights:
- Best Resolution Rate: {df.loc[df['Resolution_Rate_Percent'].idxmax(), 'Airline']} ({df['Resolution_Rate_Percent'].max():.1f}%)
- Lowest Escalation Rate: {df.loc[df['Escalation_Rate_Percent'].idxmin(), 'Airline']} ({df['Escalation_Rate_Percent'].min():.1f}%)
- Highest Satisfaction: {df.loc[df['Satisfaction_Score'].idxmax(), 'Airline']} ({df['Satisfaction_Score'].max():.1f}/5)

Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
        """
        
        st.download_button(
            label="📄 Download Report",
            data=summary_report,
            file_name=f"grievance_report_{start_date}_{end_date}.md",
            mime="text/markdown"
        )
    
    with col3:
        st.info(f"📊 Data Points: {len(df)}")

else:
    st.info("👈 Please use the sidebar to configure filters and load data")

# Footer
st.markdown("---")
st.markdown("*Dashboard last updated: " + datetime.now().strftime('%Y-%m-%d %H:%M:%S') + "*")
st.markdown("*Data source: Materialized View (Pre-aggregated daily data)*")