# By Dominic Eggerman
# Imports
import getpass
import psycopg2
import pandas as pd
import readfile


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
        return df
    except:
        conn.close()


if __name__ == "__main__":
    # Connect, query, and print df
    creds = readfile.readFile("creds.txt")
    username, password = creds[0], creds[1]
    connection = connect(username, password)
    df = getLocations(connection)

    print(len(df["id"].values))

    # Close
    connection.close()