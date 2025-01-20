
import requests
from datetime import datetime, timedelta
import pymssql

# Function to generate the start and end dates for each month
def get_month_date_range(year, month):
    start_date = datetime(year, month, 1)
    if month == 12:
        end_date = datetime(year + 1, 1, 1) - timedelta(days=1)
    else:
        end_date = datetime(year, month + 1, 1) - timedelta(days=1)
    return start_date.strftime('%Y-%m-%d'), end_date.strftime('%Y-%m-%d')

# Function to get incident data for a specific month
def get_incident_data(instance_name, auth, start_date, end_date):
    url = f"https://{instance_name}.service-now.com/api/now/stats/incident"
    query = f"resolved_atON{start_date}@javascript:gs.dateGenerate('{start_date}','start')@javascript:gs.dateGenerate('{end_date}','end')^state!=8^assignment_group.parent=524a34961b791550b9cb54ae034bcb01"
    response = requests.get(url, auth=auth, params={'sysparm_query': query, 'sysparm_count': 'true'})
    data = response.json()
    
    # Extract resolved count and total time
    resolved = int(data.get('result', {}).get('stats', {}).get('count', 0))
    total_time = float(data.get('result', {}).get('stats', {}).get('total_time', 0.0))
    return resolved, total_time

# Your ServiceNow instance name and authentication details
instance_name = 'ukgdev'
auth = ('svcManagerDashboard', '')  

# Database connection details
conn = pymssql.connect(
    server='',
    user='',  
    password='',  
    database='',
    as_dict=True
)

print("Connected to the database successfully.")

# SQL INSERT query
SQL_INSERT = """
INSERT INTO [dbo].[device_downtime] ([resolved], [total_time])
VALUES (%s, %s)
"""

# Get the current date
current_date = datetime.now()

# Database interaction
cursor = conn.cursor()

# Loop for the last 6 months
for i in range(1, 7):
    # Calculate the previous month's start date
    month_date = current_date.replace(day=1) - timedelta(days=i * 30)
    resolved = month_date.strftime('%Y-%m')  # Format it as "YYYY-MM" (e.g., "2025-01")
    
    # Get the start and end dates for the month
    start_date, end_date = get_month_date_range(month_date.year, month_date.month)
    
    # Fetch incident data (resolved count and total time)
    resolved_count, total_time = get_incident_data(instance_name, auth, start_date, end_date)
    
    # Insert into the database (passing resolved as a string and total_time as float)
    cursor.execute(SQL_INSERT, (resolved, total_time))
    print(f"Inserted data for {resolved} -> Total Time: {total_time}")

# Commit changes and close the connection
conn.commit()
cursor.close()
conn.close()
