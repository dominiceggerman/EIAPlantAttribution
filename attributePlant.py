# By Dominic Eggerman
# Imports
import getpass
import psycopg2
import pandas as pd
import json
from urllib.error import URLError, HTTPError
from urllib.request import urlopen
import readfile


# EIA API query to get data from a plant code
def EIAPlantData(key, plant_code):
    # Construct URL
    url = "http://api.eia.gov/series/?api_key={0}&series_id=ELEC.PLANT.CONS_TOT.{1}-NG-ALL.M".format(key, plant_code)

    try:
        print("Getting EIA-based nominations data for plant code {0}...".format(plant_code))
        # URL request, opener, reader
        response = urlopen(url)
        raw_byte = response.read()
        raw_string = str(raw_byte, "utf-8-sig")

        # Convert to JSON
        jso = json.loads(raw_string)

        # Convert JSON data we want to dataframe
        noms_data = jso["series"][0]["data"]
        noms_df = pd.DataFrame(data=noms_data, columns=["Month", "Noms (mcf)"])
        # Get lat/long and start/end dates
        plant_lat, plant_long = float(jso["series"][0]["lat"]), float(jso["series"][0]["lon"])
        start_month, end_month = jso["series"][0]["start"], jso["series"][0]["end"]

        # Return all as a dictionary
        return {"plant_code":plant_code, "noms_data":noms_df, "lat":plant_lat, "long":plant_long, "start_date":start_month, "end_date":end_month}

    except HTTPError as err:
        print("HTTP error...")
        print("Error code:", err.code)

    except URLError as err:
        print("URL type error...")
        print("Reason:", err.reason)


# Connect to insightprod database
def connect(usr, pswrd):
    # Establish connection with username and password
    conn = psycopg2.connect(dbname="insightprod", user=usr, password=pswrd, host="insightproddb")
    print("Successfully connected to database...")
    return conn


# Get location IDs and matching plant codes
def locationPlantMap(conn):
    # SQL statement
    statement = """SELECT lpm.location_id, plt.eia_plant_code FROM ts1.location_plant_map AS lpm
                    INNER JOIN ts1.plant AS plt ON lpm.plant_id = plt.id
                    ORDER BY location_id
                """

    try:
        # Read SQL and return
        print("Getting plant codes and location IDs...")
        df = pd.read_sql(statement, conn)
        return df
    except:
        print("locationPlantMap(): Error encountered while executing SQL. Exiting...")
        conn.close()
        return None


# Get nominations data for a single location id
def getCapacityData(conn, lat, lon, loc_id):
    # With lat/long
    if lat != None and lon != None and loc_id == None:
        # Create lat/long boundries
        latmin, latmax = lat - 0.1, lat + 0.1
        longmin, longmax = lon - 0.1, lon + 0.1
        # SQL statement
        statement = """SELECT date_trunc('month', ctnn.gas_day)::date AS date, l.name AS loc_name , SUM((ctnn.scheduled_cap + ctnn.no_notice_cap) * r.sign * -1) AS scheduled_and_nn
                        FROM analysts.captrans_with_no_notice AS ctnn
                        INNER JOIN analysts.location_role_v AS lr ON ctnn.location_role_id = lr.id
                        INNER JOIN analysts.location_v AS l ON lr.location_id = l.id
                        INNER JOIN analysts.role_v AS r ON lr.role_id = r.id
                        INNER JOIN analysts.county_v c ON l.county_id = c.id
                        INNER JOIN analysts.state_v AS s ON c.state_id = s.id
                        WHERE ctnn.gas_day BETWEEN '2014-01-01' AND '2018-06-01' 
                        AND (l.facility_id IN (8, 10, 14, 17) AND lr.role_id = 2)
                        AND l.latitude BETWEEN {0} AND {1}
                        AND l.longitude BETWEEN {2} AND {3}
                        GROUP BY 1, 2
                        HAVING max(ctnn.scheduled_cap + ctnn.no_notice_cap) > 500
                        ORDER BY 1, 2
                    """.format("'"+str(latmin)+"'", "'"+str(latmax)+"'", "'"+str(longmin)+"'", "'"+str(longmax)+"'")
                    # EDIT DATES ??
    
    # By Location ID
    elif lat is None and lon is None and loc_id != None:
        statement = """SELECT date_trunc('month', ctnn.gas_day)::date AS date, l.name AS loc_name , SUM((ctnn.scheduled_cap + ctnn.no_notice_cap) * r.sign * -1) AS scheduled_and_nn
                        FROM analysts.captrans_with_no_notice AS ctnn
                        INNER JOIN analysts.location_role_v AS lr ON ctnn.location_role_id = lr.id
                        INNER JOIN analysts.location_v AS l ON lr.location_id = l.id
                        WHERE ctnn.gas_day BETWEEN '2014-01-01' AND '2018-06-01' 
                        AND lr.id = {0}
                        GROUP BY 1, 2
                        ORDER BY 1, 2
                    """.format(loc_id)
    print(statement)
        
    try:
        # Read SQL and return
        print("Executing SQL to obtain nominations data...")
        df = pd.read_sql(statement, conn)
        return df
    except:
        print("getCapacityData(): Error encountered while executing SQL. Exiting...")
        conn.close()
        return None


if __name__ == "__main__":
    
    # Get login creds for insightprod and EIA API
    creds = readfile.readFile("creds.txt")
    username, password, eia_key = creds[0], creds[1], creds[2]

    # Connect, get location IDs and matching plant codes
    connection = connect(username, password)
    plant_locs = locationPlantMap(connection)

    plant_code = plant_locs["eia_plant_code"].values[0]

    eia_data = EIAPlantData(eia_key, plant_code)

    cap_df = getCapacityData(connection, None, None, plant_locs["location_id"].values[0])
    print(cap_df)    





    # # DAN'S QUERY
    # # Connect and get capacity data
    # connection = connect(username, password)
    # cap_df = getCapacityData(connection, eia_data["lat"], eia_data["long"], None)
    # # Close
    # connection.close()

