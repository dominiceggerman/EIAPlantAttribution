# By Dominic Eggerman
# Imports
import pandas as pd


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


if __name__ == "__main__":
    files = ["confirmed_attributions.txt", "attribution_issues.txt", "database_issues.txt"]
    readAttribs(files)