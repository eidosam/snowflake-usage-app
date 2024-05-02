
import datetime

import plotly.express as px
from snowflake.snowpark.context import get_active_session

import streamlit as st


st.set_page_config(layout="wide")

st.title("Snowflake Account Usage App :snowflake:")
st.divider()

st.markdown("Reference: https://docs.snowflake.com/en/sql-reference/account-usage#account-usage-views")
st.divider()

max_date = datetime.datetime.now()
min_date = datetime.datetime.now() - datetime.timedelta(days=365)

if "starting" not in st.session_state:
    st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=14)

if "ending" not in st.session_state:
    st.session_state.ending = max_date

st.markdown("Enter the date range (Default is 14):")

days_14_col, days_30_col, days_60_col, days_90_col, days_180_col, days_365_col = st.columns([1, 1, 1, 1, 1, 1])

with days_14_col:
    if st.button("14 Days"):
        st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=30)
        st.session_state.ending = datetime.datetime.now()

with days_30_col:
    if st.button("30 Days"):
        st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=30)
        st.session_state.ending = datetime.datetime.now()

with days_60_col:
    if st.button("60 Days"):
        st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=60)
        st.session_state.ending = datetime.datetime.now()

with days_90_col:
    if st.button("90 Days"):
        st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=90)
        st.session_state.ending = datetime.datetime.now()

with days_180_col:
    if st.button("180 Days"):
        st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=180)
        st.session_state.ending = datetime.datetime.now()

with days_365_col:
    if st.button("365 Days"):
        st.session_state.starting = datetime.datetime.now() - datetime.timedelta(days=365)
        st.session_state.ending = datetime.datetime.now()

date_input_filter = st.date_input(
    "Enter date range manually:",
    (st.session_state.starting, st.session_state.ending),
    min_date,
    max_date,
)

s, e = date_input_filter

st.divider()

session = get_active_session()

credits_used_sql = f"""
    SELECT
        ROUND(SUM(credits_used), 0) total_credits
    FROM
        snowflake.account_usage.metering_history
    WHERE
        start_time BETWEEN '{s}' AND '{e}';
"""
credits_used_df = session.sql(credits_used_sql)
pandas_credits_used_df = credits_used_df.to_pandas()
credits_used_tile = pandas_credits_used_df.iloc[0].values

num_jobs_sql = f"""
    SELECT
        COUNT(*) number_of_jobs
    FROM
        snowflake.account_usage.query_history
    WHERE
        start_time BETWEEN '{s}' AND '{e}';
"""
num_jobs_df = session.sql(num_jobs_sql)
pandas_num_jobs_df = num_jobs_df.to_pandas()
num_jobs_tile = pandas_num_jobs_df.iloc[0].values

current_storage_sql = """
    SELECT
        ROUND(AVG(storage_bytes + stage_bytes + failsafe_bytes) / POWER(1024, 4), 2) billable_tb
    FROM
        snowflake.account_usage.storage_usage
    WHERE
        usage_date = CURRENT_DATE() -1;
"""
current_storage_df = session.sql(current_storage_sql)
pandas_current_storage_df = current_storage_df.to_pandas()
current_storage_tile = pandas_current_storage_df.iloc[0].values

credits_used_col, num_jobs_col, current_storage_col = st.columns(3)
credits_used_col.metric(
    "Credits Used",
    f"{int(credits_used_tile):,}"
)
num_jobs_col.metric(
    "Total # of Jobs Executed",
    f"{int(num_jobs_tile):,}"
)
current_storage_col.metric(
    "Current Storage (TB)",
    f"{float(current_storage_tile):.2f}"
)

total_credits_used_sql = f"""
    SELECT
        warehouse_name,
        SUM(credits_used) total_credits_used
    FROM
        snowflake.account_usage.warehouse_metering_history
    WHERE
        start_time BETWEEN '{s}' AND '{e}'
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT
        10;
"""
total_credits_used_df = session.sql(total_credits_used_sql)
pandas_total_credits_used_df = total_credits_used_df.to_pandas()
fig_credits_used = px.bar(
    pandas_total_credits_used_df,
    x="TOTAL_CREDITS_USED",
    y="WAREHOUSE_NAME",
    orientation="h",
    title="Credits Used by Warehouse"
)
fig_credits_used.update_traces(marker_color="green")

jobs_by_warehouse_sql = f"""
    SELECT
        warehouse_name,
        COUNT(*) number_of_jobs
    FROM
        snowflake.account_usage.query_history
    WHERE
        start_time BETWEEN '{s}' AND '{e}'
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT
        10;
"""
jobs_by_warehouse_df = session.sql(jobs_by_warehouse_sql)
pandas_jobs_by_warehouse_df = jobs_by_warehouse_df.to_pandas()
fig_jobs_by_warehouse = px.bar(
    pandas_jobs_by_warehouse_df,
    x="NUMBER_OF_JOBS",
    y="WAREHOUSE_NAME",
    orientation="h",
    title="# of Jobs by Warehouse"
)
fig_jobs_by_warehouse.update_traces(marker_color="purple")

execution_by_qtype = f"""
    SELECT
        query_type,
        warehouse_size,
        AVG(execution_time) / 1000 average_execution_time
    FROM
        snowflake.account_usage.query_history
    WHERE
        start_time BETWEEN '{s}' AND '{e}'
    GROUP BY
        1,
        2
    ORDER by
        3 DESC;
"""
execution_by_qtype_df = session.sql(execution_by_qtype)
pandas_execution_by_qtype_df = execution_by_qtype_df.to_pandas()
fig_execution_by_qtype = px.bar(
    pandas_execution_by_qtype_df,
    x="AVERAGE_EXECUTION_TIME",
    y="QUERY_TYPE",
    orientation="h",
    title="Average Execution by Query Type"
)

container1 = st.container()

with container1:
    fig_credits_used_col, fig_jobs_by_warehouse_col, fig_execution_by_qtype_col = st.columns(3)
    with fig_credits_used_col:
        st.plotly_chart(fig_credits_used, use_container_width=True)
    with fig_jobs_by_warehouse_col:
        st.plotly_chart(fig_jobs_by_warehouse, use_container_width=True)
    with fig_execution_by_qtype_col:
        st.plotly_chart(fig_execution_by_qtype, use_container_width=True)

credits_used_overtime_sql = f"""
    SELECT
        start_time::DATE usage_date,
        warehouse_name,
        SUM(credits_used) total_credits_used
    FROM
        snowflake.account_usage.warehouse_metering_history
    where
        start_time BETWEEN '{s}' AND '{e}'
    GROUP BY
        1,
        2
    order by
        2,
        1;
"""
credits_used_overtime_df = session.sql(credits_used_overtime_sql)
pandas_credits_used_overtime_df = credits_used_overtime_df.to_pandas()

fig_credits_used_overtime_df = px.bar(
    pandas_credits_used_overtime_df,
    x="USAGE_DATE",
    y="TOTAL_CREDITS_USED",
    color="WAREHOUSE_NAME",
    orientation="v",
    title="Credits Used Overtime"
)
st.plotly_chart(fig_credits_used_overtime_df, use_container_width=True)


longest_queries_sql = f"""
    SELECT
        query_id,
        query_text,
        execution_time / 60000 exec_time
    FROM
        snowflake.account_usage.query_history
    WHERE
        execution_status = 'SUCCESS'
        AND start_time BETWEEN '{s}' AND '{e}'
    ORDER BY
        execution_time DESC
    LIMIT
        25;
"""
longest_queries_df = session.sql(longest_queries_sql)

pandas_longest_queries_df = longest_queries_df.to_pandas()

fig_longest_queries = px.bar(
    pandas_longest_queries_df,
    x="EXEC_TIME",
    y="QUERY_TEXT",
    orientation="h",
    title="Longest Successful Queries (Top 25) "
)

f_longest_queries_sql = f"""
    SELECT
        query_id,
        query_text,
        execution_time / 60000 exec_time
    FROM
        snowflake.account_usage.query_history
    WHERE
        execution_status = 'FAIL'
        AND start_time BETWEEN '{s}' AND '{e}'
    ORDER BY
        execution_time DESC
    LIMIT
        25
"""
f_longest_queries_df = session.sql(longest_queries_sql)
f_pandas_longest_queries_df = longest_queries_df.to_pandas()

fig_f_longest_queries = px.bar(
    f_pandas_longest_queries_df,
    x="EXEC_TIME",
    y="QUERY_TEXT",
    orientation="h",
    title="Longest Failed Queries (Top 25)"
)
fig_f_longest_queries.update_traces(marker_color="red")

longest_queries_container = st.container()

with longest_queries_container:
    longest_queries_col, longest_failed_queries_col = st.columns(2)
    with longest_queries_col:
        st.plotly_chart(fig_longest_queries, use_container_width=True)
    with longest_failed_queries_col:
        st.plotly_chart(fig_f_longest_queries, use_container_width=True)

warehouse_variance_sql = f"""
    SELECT
        WAREHOUSE_NAME,
        DATE(start_time) DATE,
        SUM(credits_used) CREDITS_USED,
        AVG(SUM(credits_used)) OVER (PARTITION BY warehouse_name ORDER BY date ROWS 7 PRECEDING) credits_used_7_day_avg,
        (TO_NUMERIC(SUM(credits_used)/credits_used_7_day_avg*100,10,2)-100)::STRING || '%' VARIANCE_TO_7_DAY_AVERAGE
    FROM
        snowflake.account_usage.warehouse_metering_history
    WHERE
        start_time BETWEEN '{s}' AND '{e}'
    GROUP BY
        date,
        warehouse_name
    ORDER BY
        date DESC;
"""
warehouse_variance_df = session.sql(warehouse_variance_sql)

pandas_warehouse_variance_df = warehouse_variance_df.to_pandas()

fig_warehouse_variance_df = px.bar(
    pandas_warehouse_variance_df,
    x="DATE",
    y="VARIANCE_TO_7_DAY_AVERAGE",
    color="WAREHOUSE_NAME",
    orientation="v",
    title="Warehouse Variance Greater than 7 day Average"
)
st.plotly_chart(fig_warehouse_variance_df, use_container_width=True)

total_execution_time_sql = f"""
    SELECT
        query_text,
        SUM(execution_time) / 60000 exec_time
    FROM
        snowflake.account_usage.query_history
    WHERE
        execution_status = 'SUCCESS'
        AND start_time BETWEEN '{s}' AND '{e}'
    GROUP BY
        query_text
    ORDER BY
        exec_time DESC
    LIMIT
        10;
"""
total_execution_time_df = session.sql(total_execution_time_sql).to_pandas()
fig_execution_time = px.bar(
    total_execution_time_df,
    x="EXEC_TIME",
    y="QUERY_TEXT",
    orientation="h",
    title="Total Execution Time by Repeated Queries"
)
fig_execution_time.update_traces(marker_color="LightSkyBlue")
st.plotly_chart(fig_execution_time, use_container_width=True)

credits_billed = """
    SELECT
        DATE_TRUNC('MONTH', usage_date) usage_month,
        SUM(credits_billed) credits_billed
    FROM
        snowflake.account_usage.metering_daily_history
    GROUP BY
        usage_month;
"""
credits_billed_df = session.sql(credits_billed).to_pandas()
fig_credits_billed = px.bar(
    credits_billed_df,
    x="USAGE_MONTH",
    y="CREDITS_BILLED",
    orientation="v",
    title="Credits Billed by Month"
)
st.plotly_chart(fig_credits_billed, use_container_width=True)

query_execution = """
    SELECT
        user_name,
        (AVG(execution_time)) / 1000 average_execution_time
    FROM
        snowflake.account_usage.query_history
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT
        10;
"""
query_execution_df = session.sql(query_execution).to_pandas()
fig_cquery_execution = px.bar(
    query_execution_df,
    x="USER_NAME",
    y="AVERAGE_EXECUTION_TIME",
    orientation="v",
    title="Average Execution Time per User"
)
fig_cquery_execution.update_traces(marker_color="MediumPurple")
st.plotly_chart(fig_cquery_execution, use_container_width=True)

cs_utilization = """
    SELECT
        query_type,
        SUM(credits_used_cloud_services) cs_credits,
        COUNT(1) num_queries
    FROM
        snowflake.account_usage.query_history
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT
        10;
"""
cs_utilization_df = session.sql(cs_utilization).to_pandas()
fig_cs_utilization = px.bar(
    cs_utilization_df,
    x="QUERY_TYPE",
    y="CS_CREDITS",
    orientation="v",
    title="CS Utilization by Query Type (Top 10)"
)
fig_cs_utilization.update_traces(marker_color="green")

compute_cs_by_warehouse = """
    SELECT
        warehouse_name,
        SUM(credits_used_cloud_services) credits_used_cloud_services
    FROM
        snowflake.account_usage.warehouse_metering_history
    GROUP BY
        1
    ORDER BY
        2 DESC
    LIMIT
        10;
"""
compute_cs_by_warehouse_df = session.sql(compute_cs_by_warehouse).to_pandas()
fig_compute_cs_by_warehouse = px.bar(
    compute_cs_by_warehouse_df,
    x="WAREHOUSE_NAME",
    y="CREDITS_USED_CLOUD_SERVICES",
    orientation="v",
    title="Compute AND Cloud Services by Warehouse",
    barmode="group"
)
fig_compute_cs_by_warehouse.update_traces(marker_color="purple")

cs_utilization_container = st.container()

with cs_utilization_container:
    cs_utilization_col, cs_by_warehouse_col = st.columns(2)
    with cs_utilization_col:
        st.plotly_chart(fig_cs_utilization, use_container_width=True)
    with cs_by_warehouse_col:
        st.plotly_chart(fig_compute_cs_by_warehouse, use_container_width=True)

storage_overtime = """
    SELECT
        DATE_TRUNC(month, usage_date) usage_month,
        AVG(storage_bytes + stage_bytes + failsafe_bytes) / POWER(1024, 4) billable_tb,
        AVG(storage_bytes) / POWER(1024, 4) Storage_TB,
        AVG(stage_bytes) / POWER(1024, 4) Stage_TB,
        AVG(failsafe_bytes) / POWER(1024, 4) Failsafe_TB
    FROM
        snowflake.account_usage.storage_usage
    GROUP BY
        1
    ORDER BY
        1;
"""
storage_overtime_df = session.sql(storage_overtime).to_pandas()

fig_storage_overtime = px.bar(
    storage_overtime_df,
    x="USAGE_MONTH",
    y="BILLABLE_TB",
    orientation="v",
    title="Data Storage used Overtime",
    barmode="group"
)
st.plotly_chart(fig_storage_overtime, use_container_width=True)

rows_loaded = f"""
    SELECT
        TO_TIMESTAMP(DATE_TRUNC(day, last_load_time)) usage_date,
        SUM(row_count) total_rows
    FROM
        snowflake.account_usage.load_history
    WHERE
        usage_date BETWEEN '{s}' AND '{e}'
    GROUP BY
        1
    ORDER BY
        usage_date DESC;
"""
rows_loaded_df = session.sql(rows_loaded).to_pandas()

fig_rows_loaded = px.line(
    rows_loaded_df,
    x="USAGE_DATE",
    y="TOTAL_ROWS",
    orientation="v",
    title="Rows Loaded Overtime (Copy Into)"
)
st.plotly_chart(fig_rows_loaded, use_container_width=True)

logins = """
    SELECT
        user_name,
        SUM(IFF(is_success = 'NO', 1, 0)) Failed,
        COUNT(*) Success,
        SUM(IFF(is_success = 'NO', 1, 0)) / NULLIF(COUNT(*), 0) login_failure_rate
    FROM
        snowflake.account_usage.login_history
    GROUP BY
        1
    ORDER BY
        4 DESC;
"""
logins_df = session.sql(logins).to_pandas()

fig_logins = px.bar(
    logins_df,
    x="USER_NAME",
    y="SUCCESS",
    orientation="v",
    title="Logins by User",
    barmode="group"
)
fig_logins.update_traces(marker_color="green")

logins_client = """
    SELECT
        reported_client_type Client,
        user_name,
        SUM(IFF(is_success = 'NO', 1, 0)) Failed,
        COUNT(*) Success,
        SUM(IFF(is_success = 'NO', 1, 0)) / NULLIF(COUNT(*), 0) login_failure_rate
    FROM
        snowflake.account_usage.login_history
    GROUP BY
        1,
        2
    ORDER BY
        5 DESC;
"""
logins_client_df = session.sql(logins_client).to_pandas()

fig_logins_client = px.bar(
    logins_client_df,
    x="CLIENT",
    y="SUCCESS",
    orientation="v",
    title="Logins by Client"
)
fig_logins_client.update_traces(marker_color="purple")

container_users = st.container()

with container_users:
    fig_logins_col, fig_logins_client_col = st.columns(2)
    with fig_logins_col:
        st.plotly_chart(fig_logins, use_container_width=True)
    with fig_logins_client_col:
        st.plotly_chart(fig_logins_client, use_container_width=True)

st.divider()
