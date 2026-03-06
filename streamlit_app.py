import streamlit as st
from snowflake.snowpark.context import get_active_session
from datetime import date

session = get_active_session()

st.set_page_config(page_title="Azurity Analytics", layout="wide")

st.title("Azurity Pharmaceuticals Analytics")

tab1, tab2 = st.tabs(["Email Campaign A/B Test", "Product Performance"])

with tab1:
    st.header("Email Campaign A/B Test Analysis")
    st.markdown("**Dynamic Analysis**: Adjust date ranges to analyze different campaign periods in real-time.")
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Campaign Start Date", value=date(2025, 1, 1))
    with col2:
        end_date = st.date_input("Campaign End Date", value=date(2026, 2, 28))
    
    campaign_results = session.sql(f"""
        SELECT 
            VARIANT,
            COUNT(*) AS EMAILS_SENT,
            SUM(CASE WHEN OPEN_DATE IS NOT NULL THEN 1 ELSE 0 END) AS OPENS,
            SUM(CASE WHEN CLICK_DATE IS NOT NULL THEN 1 ELSE 0 END) AS CLICKS,
            ROUND(SUM(CASE WHEN OPEN_DATE IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 2) AS OPEN_RATE,
            ROUND(SUM(CASE WHEN CLICK_DATE IS NOT NULL THEN 1 ELSE 0 END) * 100.0 / 
                  NULLIF(SUM(CASE WHEN OPEN_DATE IS NOT NULL THEN 1 ELSE 0 END), 0), 2) AS CTR
        FROM AZURITY_DEMO_DB.RAW.EMAIL_CAMPAIGNS
        WHERE SENT_DATE BETWEEN '{start_date}' AND '{end_date}'
        GROUP BY VARIANT
        ORDER BY VARIANT
    """).to_pandas()
    
    st.subheader("Campaign Performance by Variant")
    
    if len(campaign_results) >= 2:
        variant_a = campaign_results[campaign_results['VARIANT'] == 'A'].iloc[0]
        variant_b = campaign_results[campaign_results['VARIANT'] == 'B'].iloc[0]
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Variant A Open Rate", f"{variant_a['OPEN_RATE']:.1f}%")
        with col2:
            st.metric("Variant B Open Rate", f"{variant_b['OPEN_RATE']:.1f}%", 
                      delta=f"{variant_b['OPEN_RATE'] - variant_a['OPEN_RATE']:.1f}%")
        with col3:
            st.metric("Variant A CTR", f"{variant_a['CTR']:.1f}%")
        with col4:
            st.metric("Variant B CTR", f"{variant_b['CTR']:.1f}%",
                      delta=f"{variant_b['CTR'] - variant_a['CTR']:.1f}%")
        
        st.subheader("Engagement Comparison")
        chart_data = campaign_results.set_index('VARIANT')[['OPENS', 'CLICKS']]
        st.bar_chart(chart_data)
    else:
        st.warning("Not enough data for the selected date range")
    
    st.subheader("Raw Data")
    st.dataframe(campaign_results)

with tab2:
    st.header("Product Performance Trends")
    
    product_options = session.sql(
        "SELECT DISTINCT PRODUCT_NAME FROM AZURITY_DEMO_DB.RAW.PRODUCTS ORDER BY 1"
    ).to_pandas()['PRODUCT_NAME'].tolist()
    
    selected_product = st.selectbox("Select Product", product_options)
    
    product_trend = session.sql(f"""
        SELECT 
            DATE_TRUNC('week', r.FILL_DATE)::DATE AS WEEK,
            SUM(r.QUANTITY) AS WEEKLY_VOLUME,
            COUNT(DISTINCT r.HCP_ID) AS UNIQUE_PRESCRIBERS
        FROM AZURITY_DEMO_DB.RAW.PRESCRIPTIONS r
        JOIN AZURITY_DEMO_DB.RAW.PRODUCTS p ON r.PRODUCT_ID = p.PRODUCT_ID
        WHERE p.PRODUCT_NAME = '{selected_product}'
        GROUP BY 1
        ORDER BY 1
    """).to_pandas()
    
    st.subheader(f"Weekly Volume: {selected_product}")
    st.line_chart(product_trend.set_index('WEEK')['WEEKLY_VOLUME'])
    
    st.subheader(f"Unique Prescribers: {selected_product}")
    st.area_chart(product_trend.set_index('WEEK')['UNIQUE_PRESCRIBERS'])
    
    col1, col2 = st.columns(2)
    with col1:
        total_volume = product_trend['WEEKLY_VOLUME'].sum()
        st.metric("Total Volume (All Time)", f"{total_volume:,.0f}")
    with col2:
        avg_prescribers = product_trend['UNIQUE_PRESCRIBERS'].mean()
        st.metric("Avg Weekly Prescribers", f"{avg_prescribers:,.0f}")
