# By Dominic Eggerman
# Imports
import getpass
import psycopg2
import pandas as pd
import readfile
import datetime


# Connect to database
def connect(usr, pswrd):
    # Establish connection with username and password
    conn = psycopg2.connect(dbname="insightprod", user=usr, password=pswrd, host="insightproddb")
    print("Successfully connected to database...")
    return conn


# Find locations from names
def getLocations(conn):
    # Create statement to select based on location ID
    try:
        statement = """SELECT DISTINCT loc.id
                        FROM maintenance.location AS loc
                        WHERE facility_id = 17
                        ORDER BY id
                    """
        # Execute SQL and return
        df = pd.read_sql(statement, conn)
        return df["id"].values
    except:
        conn.close()


# Get nominations data for a single location id
def getCapacityData(conn, location_id):
    # Generate dates
    start_date = str((datetime.datetime.now() + datetime.timedelta(-365 * 4)).strftime("%m-%d-%Y"))  # 4 years before today
    end_date = str(datetime.datetime.now().strftime("%m-%d-%Y"))  # Today
    # SQL statement
    statement = """SELECT eod.gas_day, eod.scheduled_cap, lr.role_id
                    FROM analysts.location_role_eod_history_v AS eod
                    INNER JOIN maintenance.location_role AS lr ON eod.location_role_id = lr.id
                    INNER JOIN maintenance.location AS loc ON lr.location_id = loc.id
                    WHERE eod.gas_day BETWEEN {0} AND {1}
                    AND loc.id = {2}
                    ORDER BY eod.gas_day, lr.role_id;
                """.format("'"+start_date+"'", "'"+end_date+"'", location_id)
    # Read SQL and return
    df = pd.read_sql(statement, conn)
    return df


if __name__ == "__main__":
    
    try:
        # Connect and query for location ID's
        creds = readfile.readFile("creds.txt")
        username, password = creds[0], creds[1]
        connection = connect(username, password)
        location_ids = getLocations(connection)

        # Get capacity data
        cap_df = getCapacityData(connection, location_ids[0])
        print(cap_df)

        # Close
        connection.close()
    
    except:
        connection.close()