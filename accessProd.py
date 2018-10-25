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
        statement = """SELECT DISTINCT loc.id, plt.eia_plant_id, plt.name
                        FROM maintenance.location AS loc
                        INNER JOIN ts1.plant AS plt ON loc.id = ts1.
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
    statement = """SELECT ctnn.gas_day, SUM((ctnn.scheduled_cap + ctnn.no_notice_cap) * rv.sign *-1) AS scheduled_and_nn
                    FROM analysts.captrans_with_no_notice AS ctnn
                    INNER JOIN analysts.location_role_v AS lr ON ctnn.location_role_id = lr.id
                    INNER JOIN analysts.location_v AS lv ON lr.location_id = lv.id
                    INNER JOIN analysts.role_v AS rv ON lr.role_id = rv.id
                    WHERE ctnn.gas_day BETWEEN {0} AND {1}
                    AND l.facility_id IN
                """.format("'"+start_date+"'", "'"+end_date+"'")
    # Read SQL and return
    df = pd.read_sql(statement, conn)
    return df


if __name__ == "__main__":
    
    # Get login creds for insightprod and EIA API
    creds = readfile.readFile("creds.txt")
    username, password, eia_key = creds[0], creds[1], creds[2]

    try:
        # Connect and query for location ID's
        connection = connect(username, password)
        location_ids = getLocations(connection)

        # Get capacity data
        cap_df = getCapacityData(connection, location_ids[0])
        print(cap_df)

        # Close
        connection.close()
    
    except:
        connection.close()