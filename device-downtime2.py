import requests
import json
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

# Function to convert the time difference (hh:mm:ss) to total hours (as integer)
def convert_to_total_hours(time_str):
    try:
        # Check if the time_str is in "hh:mm:ss" format
        time_parts = time_str.split(":")
        if len(time_parts) == 3:
            hours = int(time_parts[0])
            minutes = int(time_parts[1])
            seconds = int(time_parts[2])

            # Convert to total hours as integer
            total_hours = hours + (minutes / 60) + (seconds / 3600)
            return int(total_hours)  # Return total hours as an integer
        else:
            return 0  # If the format is not as expected, return 0
    except Exception as e:
        print(f"Error in converting time: {e}")
        return 0

# Function to get incident details
def get_incident_details(instance_name, auth, start_date, end_date):
    url = f"https://{instance_name}.service-now.com/api/now/table/incident"
    query = f"resolved_atON{start_date}@javascript:gs.dateGenerate('{start_date}','start')@javascript:gs.dateGenerate('{end_date}','end')^state!=8^assignment_group.parent=524a34961b791550b9cb54ae034bcb01^ORassignment_group.parent=23f9b8561b791550b9cb54ae034bcbde^ORassignment_group.parent=2f5af4561b791550b9cb54ae034bcb2e^ORassignment_group.parent=5dabc0e787dac190bf56624e8bbb35ed^ORassignment_group=6f76a9c91b84ac1005e76572b24bcb79^ORassignment_group=d134b0d287ee4a10e7ebed3c8bbb3573^ORassignment_group=998030681306bf0034c65eff3244b05c^ORassignment_group=9d8030681306bf0034c65eff3244b056^ORassignment_group=97852104473a2910d371f19bd36d432c"
    response = requests.get(url, auth=auth, params={'sysparm_query': query, 'sysparm_fields': 'resolved_at,sys_created_on', 'sysparm_count': 'true'})
    data = response.json()
    
    resolved_count = 0
    total_time_hrs = 0

    if 'result' in data:
        incidents = data['result']
        resolved_count = len(incidents)
        
        # Calculate total time in hours based on resolved_at and sys_created_on
        for incident in incidents:
            if 'resolved_at' in incident and 'sys_created_on' in incident:
                resolved_at = datetime.strptime(incident['resolved_at'], '%Y-%m-%d %H:%M:%S')
                sys_created_on = datetime.strptime(incident['sys_created_on'], '%Y-%m-%d %H:%M:%S')
                time_diff = resolved_at - sys_created_on
                
                # Convert the time difference to total hours (as integer)
                total_time_hrs += convert_to_total_hours(str(time_diff))

    return resolved_count, total_time_hrs

# Your ServiceNow instance name and authentication details
instance_name = 'ukgdev'
auth = ('svcManagerDashboard', '}')  

# Database connection details
conn = pymssql.connect(
    server='',
    user='',  
    password='',  
    database='',
    as_dict=True
)

print("Connected to the database successfully.")

# Create the new table if it doesn't already exist
try:
    create_table_sql = """
    IF NOT EXISTS (SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = 'device-downtime-report')
    BEGIN
        CREATE TABLE [dbo].[device-downtime-report] (
            [Month] VARCHAR(7),
            [Total_Time_Hours] INT,
            [Total_Time_Emp] INT NULL
        )
    END
    """
    cursor = conn.cursor()
    cursor.execute(create_table_sql)
    print("Table [device-downtime-report] created or already exists.")
except Exception as e:
    print(f"Error creating table: {e}")
    cursor.close()
    conn.close()
    exit()

# Define the SQL INSERT query (with `Total_Time_Emp` set as NULL for now)
SQL_INSERT = """
INSERT INTO [dbo].[device-downtime-report] ([Month], [Total_Time_Hours], [Total_Time_Emp])
VALUES (%s, %d, NULL)
"""

# Get the current date
current_date = datetime.now()

# Calculate the last 6 completed months
months_to_fetch = 6
for i in range(1, months_to_fetch + 1):
    # Subtract `i` months from the current date to get the previous months
    prev_month_date = current_date.replace(day=1) - timedelta(days=i * 30)
    start_date, end_date = get_month_date_range(prev_month_date.year, prev_month_date.month)

    # Fetch incident details for the previous month
    resolved_count, total_time_hrs = get_incident_details(instance_name, auth, start_date, end_date)

    # Print out the result to check if the details are correct
    print(f"Month: {start_date[:7]}, Total Time (hrs): {total_time_hrs}")

    if resolved_count != 0:
        cursor.execute(SQL_INSERT, (start_date[:7], total_time_hrs))

# Commit the transaction and close the connection
conn.commit()
print("Data has been stored in the database successfully.")
cursor.close()
conn.close()
print("Database connection closed.")
