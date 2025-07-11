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

# Get available date range
@st.cache_data(ttl=3600)
def get_date_range():
    """Get min and max dates from the dataset"""
    query = """
    SELECT 
        MIN(inserted_date) as min_date,
        MAX(inserted_date) as max_date
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_api`
    """
    result = run_query(query)
    if result:
        return result[0]['min_date'], result[0]['max_date']
    return None, None

# Main data query function
def get_grievance_data(start_date, end_date, categories=None):
    """Fetch grievance data with partition filtering - default to Airlines"""
    # Always filter for Airlines category
    category_filter = "AND _categoryx = 'Airline'"
    
    # Add subcategory filter if specified
    subcategory_filter = ""
    if categories and len(categories) > 0:
        category_list = "', '".join(categories)
        subcategory_filter = f"AND subcategory IN ('{category_list}')"
    
    query = f"""
    SELECT 
        inserted_date,
        _categoryx,
        subcategory,
        type,
        totalreceived,
        activegrievanceswithoutescalation,
        active_grievanceswithescalation,
        closed_grievanceswithoutescalation,
        closed_grievanceswithescalation,
        successfultransferin,
        grievanceswithoutratings,
        grievanceswithratings,
        grievanceswithverygoodrating,
        grievanceswithgoodrating,
        grievanceswithokrating,
        grievanceswithbadrating,
        grievanceswithverybadrating,
        twittergrievances,
        facebookgrievances,
        grievancesadditionalinfoprovided,
        grievancesadditionalinfonotprovided,
        grievanceswithoutfeedback,
        grievanceswithfeedback,
        grievanceswithfeedbackissuenotresolved,
        grievanceswithfeedbackissueresolved
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_api`
    WHERE inserted_date >= '{start_date}'
    AND inserted_date <= '{end_date}'
    {category_filter}
    {subcategory_filter}
    ORDER BY inserted_date DESC
    """
    return run_query(query)

# Get unique airline companies (subcategories)
@st.cache_data(ttl=3600)
def get_airline_companies():
    """Get unique airline companies for filtering"""
    query = """
    SELECT DISTINCT subcategory
    FROM `upheld-setting-420306.aviation_grievances_data.aviation_grievances_api`
    WHERE _categoryx = 'Airline'
    AND subcategory IS NOT NULL
    ORDER BY subcategory
    """
    result = run_query(query)
    return [row['subcategory'] for row in result]

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
    
    # Convert to DataFrame
    df = pd.DataFrame(data)
    df['inserted_date'] = pd.to_datetime(df['inserted_date'])
    
    # Calculate derived metrics
    df['total_active'] = df['activegrievanceswithoutescalation'] + df['active_grievanceswithescalation']
    df['total_closed'] = df['closed_grievanceswithoutescalation'] + df['closed_grievanceswithescalation']
    df['resolution_rate'] = np.where(df['totalreceived'] > 0, 
                                   (df['total_closed'] / df['totalreceived']) * 100, 0)
    df['escalation_rate'] = np.where(df['totalreceived'] > 0,
                                   (df['active_grievanceswithescalation'] / df['totalreceived']) * 100, 0)
    
    # Key Metrics Row
    st.markdown("## 📊 Key Performance Indicators")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        total_received = df['totalreceived'].sum()
        st.metric("Total Grievances", f"{total_received:,}")
    
    with col2:
        total_active = df['total_active'].sum()
        st.metric("Active Grievances", f"{total_active:,}")
    
    with col3:
        total_closed = df['total_closed'].sum()
        st.metric("Closed Grievances", f"{total_closed:,}")
    
    with col4:
        avg_resolution_rate = df['resolution_rate'].mean()
        st.metric("Avg Resolution Rate", f"{avg_resolution_rate:.1f}%")
    
    with col5:
        avg_escalation_rate = df['escalation_rate'].mean()
        st.metric("Avg Escalation Rate", f"{avg_escalation_rate:.1f}%")
    
    # Create visualizations
    st.markdown("## 📈 Analytics & Insights")
    
    # Row 1: Time series and category breakdown
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 📅 Grievances Over Time")
        daily_stats = df.groupby('inserted_date').agg({
            'totalreceived': 'sum',
            'total_active': 'sum',
            'total_closed': 'sum'
        }).reset_index()
        
        fig_time = go.Figure()
        fig_time.add_trace(go.Scatter(
            x=daily_stats['inserted_date'],
            y=daily_stats['totalreceived'],
            name='Total Received',
            line=dict(color='#1f77b4', width=3)
        ))
        fig_time.add_trace(go.Scatter(
            x=daily_stats['inserted_date'],
            y=daily_stats['total_active'],
            name='Active',
            line=dict(color='#ff7f0e', width=2)
        ))
        fig_time.add_trace(go.Scatter(
            x=daily_stats['inserted_date'],
            y=daily_stats['total_closed'],
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
        st.markdown("### ✈️ Airlines Distribution")
        airline_stats = df.groupby('subcategory')['totalreceived'].sum().reset_index()
        airline_stats = airline_stats.sort_values('totalreceived', ascending=False)
        
        fig_airline = px.pie(
            airline_stats,
            values='totalreceived',
            names='subcategory',
            title="Total Grievances by Airline"
        )
        fig_airline.update_layout(height=400)
        st.plotly_chart(fig_airline, use_container_width=True)
    
    # Row 2: Rating analysis and resolution metrics
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### ⭐ Rating Distribution")
        rating_cols = ['grievanceswithverygoodrating', 'grievanceswithgoodrating', 
                      'grievanceswithokrating', 'grievanceswithbadrating', 'grievanceswithverybadrating']
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
    
    with col2:
        st.markdown("### 📱 Social Media Grievances")
        social_data = {
            'Platform': ['Twitter', 'Facebook'],
            'Grievances': [df['twittergrievances'].sum(), df['facebookgrievances'].sum()]
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
    
    # Row 3: Resolution and feedback analysis
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### 🔄 Airlines Resolution Analysis")
        resolution_data = df.groupby('subcategory').agg({
            'totalreceived': 'sum',
            'total_closed': 'sum',
            'resolution_rate': 'mean'
        }).reset_index()
        
        fig_resolution = px.scatter(
            resolution_data,
            x='totalreceived',
            y='resolution_rate',
            size='total_closed',
            color='subcategory',
            title="Resolution Rate vs Total Grievances by Airline",
            labels={'totalreceived': 'Total Received', 'resolution_rate': 'Resolution Rate (%)', 'subcategory': 'Airline'}
        )
        fig_resolution.update_layout(height=400)
        st.plotly_chart(fig_resolution, use_container_width=True)
    
    with col2:
        st.markdown("### 💬 Feedback Analysis")
        feedback_resolved = df['grievanceswithfeedbackissueresolved'].sum()
        feedback_unresolved = df['grievanceswithfeedbackissuenotresolved'].sum()
        
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
    
    # Detailed Data Table
    st.markdown("## 📋 Detailed Data")
    
    # Add filters for the table
    col1, col2 = st.columns(2)
    with col1:
        show_only_active = st.checkbox("Show only active grievances")
    with col2:
        min_grievances = st.slider("Minimum grievances", 0, int(df['totalreceived'].max()), 0)
    
    # Filter data for table
    table_df = df.copy()
    if show_only_active:
        table_df = table_df[table_df['total_active'] > 0]
    if min_grievances > 0:
        table_df = table_df[table_df['totalreceived'] >= min_grievances]
    
    # Select columns for display
    display_cols = ['inserted_date', 'subcategory', 'totalreceived', 
               'total_active', 'total_closed', 'resolution_rate', 'escalation_rate']
    st.dataframe(
        table_df[display_cols].sort_values('inserted_date', ascending=False),
        use_container_width=True,
        height=400
    )
    
    # Summary statistics
    st.markdown("## 📊 Summary Statistics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("### Airlines Performance")
        airline_summary = df.groupby('subcategory').agg({
            'totalreceived': ['sum', 'mean'],
            'resolution_rate': 'mean',
            'escalation_rate': 'mean'
        }).round(2)
        airline_summary.columns = ['Total', 'Daily Avg', 'Avg Resolution %', 'Avg Escalation %']
        st.dataframe(airline_summary)
    with col2:
        st.markdown("### Overall Metrics")
        overall_stats = {
            'Metric': ['Total Grievances', 'Average Daily Grievances', 'Peak Daily Grievances', 
                      'Overall Resolution Rate', 'Overall Escalation Rate'],
            'Value': [
                f"{df['totalreceived'].sum():,}",
                f"{df['totalreceived'].mean():.1f}",
                f"{df['totalreceived'].max():,}",
                f"{(df['total_closed'].sum() / df['totalreceived'].sum() * 100):.1f}%",
                f"{(df['active_grievanceswithescalation'].sum() / df['totalreceived'].sum() * 100):.1f}%"
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

            ### Top Airlines:
            {airline_stats.head().to_string(index=False)}

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