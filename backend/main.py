from datetime import datetime
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
import pytz
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI()

origins = [
    "http://127.0.0.1:5500",
    # Add more allowed origins if needed
]

# Configure CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],  # You can restrict to specific HTTP methods if needed
    allow_headers=["*"],  # You can restrict to specific HTTP headers if needed
)


cluster = Cluster(['localhost'])  # Replace with your Cassandra node IP
session = cluster.connect('spark_streams')  # Replace with your keyspace name

def process_dataframe_for_last(df, time_range='6_hours'):
    desired_timezone = 'Asia/Bangkok'
    df['timestamp'] = pd.to_datetime('now').tz_localize(pytz.timezone(desired_timezone)).strftime('%Y-%m-%d %H:%M:%S')
    # Filter data for the specified time range
    df['timestamp'] = pd.to_datetime(df['timestamp'])

    end_time = pd.to_datetime('now')
    timedelta = 10
    # Determine the start time based on the specified time range
    if time_range == '6_hours':
        end_time = end_time.replace(second=0, microsecond=0, minute=0, hour=end_time.hour+1)
        start_time = (end_time - pd.Timedelta(hours=6))
        
    elif time_range == '1_day':
        end_time = pd.to_datetime(end_time.date()) + pd.DateOffset(days=1) - pd.Timedelta(seconds=1)
        start_time = pd.to_datetime((end_time - pd.Timedelta(days=1)).date())
        timedelta = 60
    elif time_range == '7_day':
        end_time = pd.to_datetime(end_time.date()) + pd.DateOffset(days=1) - pd.Timedelta(seconds=1)
        timedelta = 60 * 24
        start_time = pd.to_datetime((end_time - pd.Timedelta(days=8)).date())
    else:
        raise ValueError("Invalid time_range. Supported values are '6_hours' and 'one_day'.")


   
    # Filter data for the specified time range
    df_filtered = df[df['timestamp'] >= start_time]

     # Convert 'timestamp' to DateTime object

    if time_range == '6_hours':
        df_filtered['formatted_timestamp'] = df_filtered['timestamp'].dt.round('10min')
    elif time_range == '1_day':
        df_filtered['formatted_timestamp'] = df_filtered['timestamp'].dt.floor('H')
    elif time_range == '7_day':
        df_filtered['formatted_timestamp'] = df_filtered['timestamp'].dt.floor('D')

    # Group by formatted timestamp and calculate count
    grouped_df = df_filtered.groupby('formatted_timestamp')['count'].sum().reset_index()

    chart_data = {
        'timestamp': [],
        'count': []
    }

    def formatTimestime(timestamp): 
        if time_range == '6_hours':
            return timestamp.strftime('%H:%M')
        elif time_range == '1_day':
            return timestamp.strftime('%H')
        else:
            return timestamp.strftime('%Y-%m-%d')

    current_time = start_time
    while current_time <= end_time:
        count_value = grouped_df.loc[grouped_df['formatted_timestamp'] == current_time, 'count'].values
        chart_data['count'].append(count_value[0] if len(count_value) > 0 else 0)
        chart_data['timestamp'].append(formatTimestime(current_time))
        current_time += pd.Timedelta(minutes=timedelta)       
    
    return pd.DataFrame(chart_data)

def query_request_excutor(session, project_id, from_timestamp):
    query = f"""
        SELECT * FROM aggregated_api_request
        WHERE project_id = '{project_id}'
        AND timestamp >= '{from_timestamp}'
        ;
    """
    statement = SimpleStatement(query)
    result = session.execute(statement)

    return result

# Close the session when the application shuts down
@app.on_event("shutdown")
def shutdown_event():
    session.shutdown()

@app.get("/api/request-chart")
async def get_request_count(project_id: str = None, range: str='6_hours'):
    # Filter DataFrame based on parameters
    start = "2023-11-01 00:00:00.000"
    result = query_request_excutor(session, project_id, start)

    # Calculate request count
    df = pd.DataFrame(result)
    aggdf = process_dataframe_for_last(df, range)
    result_dict = {
        'timestamp': aggdf['timestamp'].tolist(),
        'count': aggdf['count'].tolist()
    }

    return JSONResponse(content=result_dict)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)

@app.get("/api/total-chart")
async def get_request_count(project_id: str = None):
    query = f"""
        SELECT * FROM aggregated_api_request
        WHERE project_id = '{project_id}'
        AND timestamp >= '{from_timestamp}'
        ;
    """
    statement = SimpleStatement(query)
    result = session.execute(statement)

    # Calculate request count
    df = pd.DataFrame(result)
    aggdf = process_dataframe_for_last(df, range)
    result_dict = {
        'timestamp': aggdf['timestamp'].tolist(),
        'count': aggdf['count'].tolist()
    }

    return JSONResponse(content=result_dict)

if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8000)
