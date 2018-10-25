# By Dominic Eggerman
# Imports
import getpass
import psycopg2
import pandas as pd


# Connect to database
def connect(usr, pswrd):
    # Establish connection with username and password
    conn = psycopg2.connect(dbname="insightprod", user=usr, password=pswrd, host="insightproddb")
    print("Successfully connected to database...")
    return conn


# Find location id's from names
def getLocationIDs(conn):
    # Create statement to select based on location ID
    try:
        point = int(point)
        statement = """  """.format(point, pipe_id)
        print("Querying database for points matching id = {0}".format(point))
    # Execute SQL
    df = pd.read_sql(statement, conn)


if __name__ == "__main__":
    # Connect, query, and print df
    connection = connect(username, password)

    # Close
    connection.close()