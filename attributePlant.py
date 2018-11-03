# By Dominic Eggerman
# Imports
import getpass
import psycopg2
import pandas as pd
import numpy as np
import json
import datetime
import argparse
import sklearn.metrics as sk
import matplotlib.pyplot as plt
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
        noms_df = pd.DataFrame(data=noms_data, columns=["eia_date", "eia_noms"])
        noms_df = noms_df.iloc[::-1]  # Reverse df - oldest to newest
        dates = [datetime.datetime.strptime("{0}-{1}-{2}".format(s[:4], s[4:6], "01"), "%Y-%m-%d").date() for s in noms_df["eia_date"].values]  # Convert string to datetime
        noms_df = noms_df.replace(noms_df["eia_date"].values, dates)
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
    statement = """SELECT date_trunc('month', ctnn.gas_day)::date AS insight_date, l.name AS loc_name, SUM((ctnn.scheduled_cap + ctnn.no_notice_cap) * r.sign * -1) AS insight_noms
                    FROM analysts.captrans_with_no_notice AS ctnn
                    INNER JOIN analysts.location_role_v AS lr ON ctnn.location_role_id = lr.id
                    INNER JOIN analysts.location_v AS l ON lr.location_id = l.id
                    INNER JOIN analysts.role_v AS r ON lr.role_id = r.id
                    INNER JOIN analysts.county_v AS c ON l.county_id = c.id
                    INNER JOIN analysts.state_v AS s ON c.state_id = s.id
                    WHERE ctnn.gas_day BETWEEN '2014-01-01' AND '2018-06-01' 
                    AND l.id = {0}
                    GROUP BY 1, 2
                    ORDER BY 1, 2
                """.format(loc_id)
        
    try:
        # Read SQL and return
        print("Executing SQL to obtain nominations data from insightprod...")
        df = pd.read_sql(statement, conn)
        return df.drop(["loc_name"], axis=1)
    except:
        print("getCapacityData(): Error encountered while executing SQL. Exiting...")
        conn.close()
        return None


# Plot EIA data versus insight data
def plotNominations(df, loc, plt_code, r2):
    # Plot
    ax = plt.axes()
    ax.plot(merged_df["eia_date"].values, merged_df["eia_noms"].values)
    ax.plot(merged_df["eia_date"].values, merged_df["insight_noms"].values)
    # Title / axis labels / legend / r2 value
    plt.title("Location ID: {0} Plant code: {1}".format(loc, plt_code))
    plt.ylabel("Mcf/d")
    plt.xticks(rotation=90)
    legend = plt.legend(["EIA data", "Insight data"], frameon=False)
    legend.draggable()
    plt.text(0.9, 1.05, "$R^2$ = {:.4f}".format(r2), ha="center", va="center", transform=ax.transAxes)
    # Fix layout and show
    plt.tight_layout()
    plt.show()



if __name__ == "__main__":
    # Argparse and add arguments
    parser = argparse.ArgumentParser(description="Below is a list of optional arguements with descriptions. Please refer to README.md for full documentation...")
    parser.add_argument("-g", "--graph", help="Do not display graph.", action="store_false")
    parser.add_argument("-s", "--skip", help="Skip previously analyzed plants", action="store_false")
    options = parser.parse_args()
    
    # Get login creds for insightprod and EIA API
    creds = readfile.readFile("creds.txt")
    username, password, eia_key = creds[0], creds[1], creds[2]

    # Connect, get location IDs and matching plant codes
    connection = connect(username, password)
    try:
        plant_locs = locationPlantMap(connection)
    except:
        connection.close()
        print("Error encountered while querying for plant locations and codes.")
    print("Found ")

    # Iterate through the "confirmed" plants
    for (location_id, plant_code) in zip(plant_locs["location_id"].values, plant_locs["eia_plant_code"].values):
        try:
            # Obtain EIA and insight data
            eia_data = EIAPlantData(eia_key, plant_code)
            cap_data = getCapacityData(connection, None, None, location_id)
        except:
            connection.close()
            print("Error accessing EIA / insight nominations data.")

        try:
            # Merge dataframes
            merged_df = eia_data["noms_data"].join(cap_data.set_index("insight_date"), on="eia_date")
            # Take only rows with non-NaN values
            merged_df = merged_df[pd.notnull(merged_df['insight_noms'])]
            # Check length of array
            if len(merged_df["insight_noms"].values) <= 5: # What number should go here??
                pass
                # Logic for handling in loop
        except ValueError:
            print("Encountered ValueError...")
            if len(cap_data["insight_noms"].values) == 0:
                print("Error: Insight nominations has no values.")

        # Score the R squared
        r2 = sk.r2_score(merged_df["eia_noms"].values, merged_df["insight_noms"].values) # May have to drop a certain value ??

        # Plot the results
        if options.graph:
            plotNominations(merged_df, location_id, plant_code, r2)

        # Ask to confirm attribution
        save_it = input("Confirm this attribution (y/n): ")
        if save_it == "y" or save_it == "yes":
            with open("confirmed_attributions.txt", mode="w") as logfile:
                logfile.write("loc_id : {} | plant_code : {} | R2 : {:.4f} | date_att: {}\n".format(location_id, plant_code, r2, datetime.datetime.now().date()))
        elif save_it == "n" or save_it == "no":
            with open("attribution_issues.txt", mode="w") as logfile:
                logfile.write("loc_id : {} | plant_code : {} | R2 : {:.4f} | date_att: {}\n".format(location_id, plant_code, r2, datetime.datetime.now().date()))
        else:
            print("Point not confirmed or unconfirmed...")
        