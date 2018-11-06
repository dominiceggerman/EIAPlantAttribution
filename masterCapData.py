# By Dominic Eggerman
# Imports
import pandas as pd
from attributePlant import connect as conn
import readfile


# Get capacity data for all matched plants
def getAllCapData(connection):
    statement = """SELECT date_trunc('month', ctnn.gas_day)::date AS insight_date, lpm.location_id AS location_id, plt.eia_plant_code AS plant_code, SUM((ctnn.scheduled_cap + ctnn.no_notice_cap) * r.sign * -1) AS insight_noms
                    FROM analysts.captrans_with_no_notice AS ctnn
                    INNER JOIN analysts.location_role_v AS lr ON ctnn.location_role_id = lr.id
                    INNER JOIN analysts.location_v AS l ON lr.location_id = l.id
                    INNER JOIN analysts.role_v AS r ON lr.role_id = r.id
                    INNER JOIN ts1.location_plant_map AS lpm ON lpm.location_id = l.id
                    INNER JOIN ts1.plant AS plt ON plt.id = lpm.plant_id
                    WHERE ctnn.gas_day BETWEEN '2014-01-01' AND '2018-05-31'
                    GROUP BY 1, 2, 3
                    ORDER BY 3, 2, 1
                """

    try:
        # Read SQL and return
        print("Excecuting query...")
        df = pd.read_sql(statement, connection)
        df.to_csv("masterCapData.csv", index=False)
    except:
        print("getCapacityData(): Error encountered while executing SQL. Exiting...")
        connection.close()
        return None


if __name__ == "__main__":
    creds = readfile.readFile("creds.txt")
    username, password = creds[0], creds[1]
    connection = conn(username, password)
    getAllCapData(connection)