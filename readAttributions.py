# By Dominic Eggerman
# Imports
import pandas as pd
import psycopg2
from attributePlant import connect as conn
import readfile
import argparse

# Read Attributions to csv
def readAttribs(files):
    with open("attribResults.csv", mode="a") as out_file:
        out_file.write("date_attr,plant_code,loc_id,R2,status\n")
        for f in files:
            with open(f, mode="r") as read_file:
                for line in read_file:
                    # Read the entry, split the data
                    entry = line.rstrip().split("|")
                    date = entry[3].split(":")[1].strip()
                    plant = entry[1].split(":")[1].strip()
                    loc_id = entry[0].split(":")[1].strip()
                    r2 = entry[2].split(":")[1].strip()

                    try:
                        if float(r2) >= 0.50:
                            status = "good"
                        elif float(r2) < 0.50:
                            status = "bad"
                    except:
                        status = "N/A"

                    # Write to file
                    line = date + "," + plant + "," + loc_id + "," + r2 + "," + status + "\n"
                    out_file.write(line)


# Assign attributions to analyst
def assignAttribs():
    # Read creds
    creds = readfile.readFile("creds.txt")
    username, password = creds[0], creds[1]
    # Read attribResults.csv
    attrib_df = pd.read_csv("attribResults.csv")

    with open("plantAttribChecks.csv", mode="a") as out_file:
        # Write out header
        out_file.write("Analyst,Pipeline,Location Name,Location ID / IDs,Facility Type,County,State,Assigned Plant Code,Assigned R2\n")
        for ind, row in attrib_df.iterrows():
            print(ind, "/", len(attrib_df["loc_id"].values))
            try:
                if pd.read_csv("plantAttribChecks.csv")["Location ID / IDs"].values[ind].split(";")[0] == str(row[2].split(";")[0]):
                    continue
            except:
                pass
            try:
                supp_df = supplementalSQL(username, password, row[2].split(";")[0])
                line = str(supp_df["analyst"][0]) + "," + str(supp_df["pipe_id"][0]) + "," + str(supp_df["loc_name"][0].replace(",", " ")) + ","  + str(row[2]) + "," + str(supp_df["facility_id"][0]) + "," + str(supp_df["county"][0]) + "," + str(supp_df["state"][0]) + "," + str(row[1]) + "," + str(row[3]) + "\n"
            except:
                line = "NA,NA,NA," + str(row[2]) + ",NA,NA,NA," + str(row[1]) + "," + str(row[3]) + "\n"
            
            out_file.write(line)
        
        out_file.close()



# Supplemental query to get analyst, etc.
def supplementalSQL(username, password, loc):
    # Establish connection and query
    link = conn(username, password)
    statement = """SELECT pa.value as analyst, loc.pipeline_id as pipe_id, loc.name as loc_name, loc.facility_id, loc.county, loc.state
                    FROM maintenance.location AS loc
                    INNER JOIN maintenance.pipeline_attribute AS pa ON pa.pipeline_id = loc.pipeline_id
                    WHERE loc.id IN ({0})
                    AND pa.value IN ('Dominic', 'Dan', 'Cory', 'Alisson', 'Vanessa', 'Joe', 'Matt', 'Anthony', 'Josh')
                """.format(loc)
    df = pd.read_sql(statement, link)
    link.close()
    return df


if __name__ == "__main__":
    # Argparse and add arguments
    parser = argparse.ArgumentParser(description="Below is a list of optional arguements with descriptions. Please refer to README.md for full documentation...")
    parser.add_argument("-r", "--read", help="Do not read from 3 base files.", action="store_false")
    options = parser.parse_args()

    if options.read:
        # Read attributions to prelimanary master file
        files = ["confirmed_attributions.txt", "attribution_issues.txt", "database_issues.txt"]
        readAttribs(files)

    assignAttribs()