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

    except KeyError:
        return None


# Connect to insightprod database
def connect(usr, pswrd):
    # Establish connection with username and password
    conn = psycopg2.connect(dbname="insightprod", user=usr, password=pswrd, host="insightproddb")
    # print("Successfully connected to database...")
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
def getCapacityData(conn, plt_id):
    statement = """SELECT date_trunc('month', ctnn.gas_day)::date AS insight_date, SUM((ctnn.scheduled_cap + ctnn.no_notice_cap) * r.sign * -1) AS insight_noms
                    FROM analysts.captrans_with_no_notice AS ctnn
                    INNER JOIN analysts.location_role_v AS lr ON ctnn.location_role_id = lr.id
                    INNER JOIN analysts.location_v AS l ON lr.location_id = l.id
                    INNER JOIN analysts.role_v AS r ON lr.role_id = r.id
                    INNER JOIN ts1.location_plant_map AS lpm ON lpm.location_id = l.id
                    INNER JOIN ts1.plant AS plt ON plt.id = lpm.plant_id
                    WHERE ctnn.gas_day BETWEEN '2014-01-01' AND '2018-05-31'
                    AND plt.eia_plant_code = {0}
                    GROUP BY 1
                """.format(plt_id)
        
    try:
        # Read SQL and return
        df = pd.read_sql(statement, conn)
        return df
    except:
        print("getCapacityData(): Error encountered while executing SQL. Exiting...")
        conn.close()
        return None


# Get plants that have already been analyze
def analyzedPlants():
    analyzed_locs = []
    with open("attribution_issues.txt", mode="r") as file1:
        for line in file1:
            try:
                loc = line.rstrip().split("|")[0].split(":")[1].strip()
                analyzed_locs.append(int(loc))
            except IndexError:
                pass
    with open("confirmed_attributions.txt", mode="r") as file2:
        for line in file2:
            try:
                loc = line.rstrip().split("|")[0].split(":")[1].strip()
                analyzed_locs.append(int(loc))
            except IndexError:
                pass
    with open("database_issues.txt", mode="r") as file3:
        for line in file3:
            try:
                loc = line.rstrip().split("|")[0].split(":")[1].strip()
                analyzed_locs.append(int(loc))
            except IndexError:
                pass
    return analyzed_locs


# Merge EIA and insight dataframes
def mergeDf(eia, insight):
    # Merge dataframes
    merged_df = eia["noms_data"].join(insight.set_index("insight_date"), on="eia_date")
    # Take only rows with non-NaN values
    merged_df = merged_df[pd.notnull(merged_df['insight_noms'])]
    # Check length of array
    if len(merged_df["insight_noms"].values) <= 5: # What number should go here??
        pass
        # Logic for handling in loop

    return merged_df


# Score the r2 of a merged dataframe
def scoreR2(df):
    try:
        # Score the R squared
        r2 = sk.r2_score(df["eia_noms"].values, df["insight_noms"].values)
        return r2
    except ValueError:
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
    parser.add_argument("-m", "--master", help="Use masterCapData.csv to get insight noms (faster).", action="store_false")
    options = parser.parse_args()

    # Get login creds for insightprod and EIA API
    creds = readfile.readFile("creds.txt")
    username, password, eia_key = creds[0], creds[1], creds[2]
    
    # Refactor all this ??
    # Use master file to compare insight data to EIA
    if options.master:
        # Read master data file
        master_df = pd.read_csv("masterCapData.csv")

        # Iterate through unique EIA plant codes
        for ind, plant in enumerate(list(set(master_df["plant_code"].values))):
            print("Analyzing plant: {0} | {1}/{2}".format(plant, ind, len(list(set(master_df["plant_code"].values)))))

            # Filter the data for a single plant
            cap_data = master_df.loc[master_df["plant_code"] == plant]
            # Get location ID / ID's
            location_id = list(set(cap_data["location_id"].values))
            # Drop unnecessary columns and convert dates from str to datetime
            cap_data = cap_data.drop(columns=["location_id", "plant_code"])
            dates = [datetime.datetime.strptime("{0}-{1}-{2}".format(d[:4], d[5:7], d[8:10]), "%Y-%m-%d").date() for d in cap_data["insight_date"].values]
            cap_data = cap_data.replace(cap_data["insight_date"].values, dates)

            # Obtain EIA data
            eia_data = EIAPlantData(eia_key, plant)
            if eia_data is None:
                print("EIA data error.")
                with open("database_issues.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : undefined | date_att: {}\n".format(tuple(location_id), plant, datetime.datetime.now().date()))
                continue

            # Merge the dataframes
            merged_df = mergeDf(eia_data, cap_data)

            # Score the r2
            r2 = scoreR2(merged_df)
            if r2 is None:
                print("No overlapping values on which to grade r2.")
                with open("database_issues.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : undefined | date_att: {}\n".format(tuple(location_id), plant, datetime.datetime.now().date()))
                continue

            # Plot the results
            if options.graph:
                plotNominations(merged_df, location_id, plant, r2)

            # Confirm / reject attribution
            if r2 >= 0.50:
                print("Attribution confirmed (r2 > 50)")
                with open("confirmed_attributions.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : {:.4f} | date_att: {}\n".format(tuple(location_id), plant, r2, datetime.datetime.now().date()))
            elif r2 < 0.50:
                print("Attribution issue (r2 < 50)")
                with open("attribution_issues.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : {:.4f} | date_att: {}\n".format(tuple(location_id), plant, r2, datetime.datetime.now().date()))
            else:
                print("Point not confirmed or unconfirmed...")


    # Run a query each time
    else:
        # Connect, get location IDs and matching plant codes
        connection = connect(username, password)
        try:
            plant_locs = locationPlantMap(connection)
            print("Found {0} attributed plants in insightprod".format(len(plant_locs["location_id"].values)))
        except:
            connection.close()
            print("Error encountered while querying for plant locations and codes.")

        # Remove plants from list that have already been analyzed
        analyzed_locs = analyzedPlants()
        for loc in analyzed_locs:
            plant_locs = plant_locs[plant_locs.location_id != loc]
        
        print("{0} plants have not been analyzed".format(len(plant_locs["location_id"].values)))

        # Close connection
        connection.close()

        # Iterate through the "confirmed" plants
        for ind, (location_id, plant_code) in enumerate(zip(plant_locs["location_id"].values, plant_locs["eia_plant_code"].values)):
            # Open connection
            connection = connect(username, password)

            print("| Analyzing Plant {0} / {1} |".format(ind+1, len(plant_locs["location_id"].values)))
            try:
                # Obtain EIA and insight data
                eia_data = EIAPlantData(eia_key, plant_code)
                cap_data = getCapacityData(connection, plant_code)
            except:
                connection.close()
                print("Error accessing EIA / insight nominations data.")

            # Error Check
            if cap_data is None:
                print("No capacity data returned.")
                with open("database_issues.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : undefined | date_att: {}\n".format(location_id, plant_code, datetime.datetime.now().date()))
                continue

            # Merge the dataframes
            merged_df = mergeDf(eia_data, cap_data)

            # Score the r2
            r2 = scoreR2(merged_df)
            if r2 is None:
                print("No overlapping values on which to grade r2.")
                with open("database_issues.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : undefined | date_att: {}\n".format(location_id, plant_code, datetime.datetime.now().date()))
                continue

            # Plot the results
            if options.graph:
                plotNominations(merged_df, location_id, plant_code, r2)

            # Confirm / reject attribution
            if r2 >= 0.50:
                print("Attribution confirmed (r2 > 50)")
                with open("confirmed_attributions.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : {:.4f} | date_att: {}\n".format(location_id, plant_code, r2, datetime.datetime.now().date()))
            elif r2 < 0.50:
                print("Attribution issue (r2 < 50)")
                with open("attribution_issues.txt", mode="a") as logfile:
                    logfile.write("loc_id : {} | plant_code : {} | R2 : {:.4f} | date_att: {}\n".format(location_id, plant_code, r2, datetime.datetime.now().date()))
            else:
                print("Point not confirmed or unconfirmed...")
            
            # Close connection
            connection.close()