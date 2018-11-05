# EIAPlantAttribution
Matching gas pipe nominations to EIA database plants.

## Additional Modules
To run this code, you will need the `psycopg2` module which does not come standard with Anaconda.  To install this, type `pip install psycopg2` in the (anaconda prompt) terminal.

## Setup
To run this code, you will need to first register with the EIA to obtain a key and access their API.  To obtain the EIA key:
- Go to https://www.eia.gov/opendata/ and register.
- When you receive the key, add it to the `creds.txt` file so the code can use the key to access EIA data.

## Credentials
The credentials file contains the following:

```

username: XXXXX
password: YYYYY
eia_key: ZZZZZZZZZZZZZZZZZZZZZZZZZZ

```

Username and password are your credentials for logging into Genscape databases, and the eia_key is the eia key you obtain after regestering with the EIA.