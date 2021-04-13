import requests
import json
import psycopg2
from psycopg2 import Error


url = "https://api.covid19india.org/state_district_wise.json"

r = requests.get(url = url)

data = r.json()

try:
    connection = psycopg2.connect(user="postgres", password="root", host="127.0.0.1", port="5432", database="covid19")
    print("PostgreSQL server")
    print(connection.get_dsn_parameters(), "\n")
    # data = json.loads(data)
    for state, district in data.items():
        cursor = connection.cursor()
        state = state
        statecode = str()
        print(state)
        print("DATA", district)
        print("-----------------")
        if isinstance(district, dict):
            for key, value in district.items():
                if key == "statecode":
                    print("STATECODE :", value)
                    statecode = value
                elif isinstance(value, dict):
                    print("DATA :", value)
                    if isinstance(value, dict):
                        district_name_ = str()
                        active = int()
                        confirmed = int()
                        deceased = int()
                        recovered = int()
                        delta_confirmed = int()
                        delta_deceased = int()
                        delta_recovered = int()
                        for district_name, cases in value.items():
                            print("DISTRICT NAME :", district_name)
                            district_name_ = district_name
                            print("CASES :", cases)
                            for i, j in cases.items():
                                print("TYPE :", i)
                                if i == "delta":
                                    for delta_name, delta_number in j.items():
                                        if delta_name == "confirmed":
                                            delta_confirmed = delta_number
                                        elif delta_name == "deceased":
                                            delta_deceased = delta_number
                                        elif delta_name == "recovered":
                                            delta_recovered = delta_number 
                                        print("DELTAS :", delta_name, delta_number)
                                else:
                                    if i == "active":
                                        active = j
                                    elif i == "confirmed":
                                        confirmed = j
                                    elif i == "deceased":
                                        deceased = j
                                    elif i == "recovered":
                                        recovered = j
                                    print("COUNTS :", j)
                                    st_district = str(statecode)+ str(district_name_).replace(" ", "")
                                    cursor.execute(f"INSERT INTO state_district_wise (state, district, active, confirmed, deceased, recovered, delta_confirmed, delta_deceased, delta_recovered, statecode, st_district) VALUES('{state}', '{district_name_}', {active}, {confirmed}, {deceased}, {recovered}, {delta_confirmed}, {delta_deceased}, {delta_recovered}, '{statecode}', '{st_district}') ON CONFLICT (st_district) \
                                        DO UPDATE SET active = {active}, confirmed = {confirmed}, deceased = {deceased}, recovered = {recovered}, delta_confirmed = {delta_confirmed}, \
                                           delta_deceased = {delta_deceased}, delta_recovered = {delta_recovered};")
                                    # cursor.execute(f"INSERT into state_district_wise (state, district, active, confirmed, deceased, recovered, delta_confirmed, delta_deceased, delta_recovered, statecode) VALUES('{state}', '{district_name_}', {active}, {confirmed}, {deceased}, {recovered}, {delta_confirmed}, {delta_deceased}, {delta_recovered}, '{statecode}')")
                                print("----------------------------")
            connection.commit()
            cursor.close()
    print("PostgreSQL connection is closed")
except (Exception, Error) as error:
    print("Error while connecting to PostgreSQL", error)
    
    
    
    
# -------------------------- DATABASE CREATION SCRIPT ----------------------------- #
# -- Database: covid19

# -- DROP DATABASE covid19;

# CREATE DATABASE covid19
#     WITH 
#     OWNER = postgres
#     ENCODING = 'UTF8'
#     LC_COLLATE = 'English_India.1252'
#     LC_CTYPE = 'English_India.1252'
#     TABLESPACE = pg_default
#     CONNECTION LIMIT = -1;
# -- Table: public.state_district_wise

# -- DROP TABLE public.state_district_wise;

# CREATE TABLE public.state_district_wise
# (
#     state character varying(150) COLLATE pg_catalog."default",
#     recovered integer,
#     district character varying(150) COLLATE pg_catalog."default",
#     delta_deceased integer,
#     delta_confirmed integer,
#     deceased integer,
#     confirmed integer,
#     active integer,
#     delta_recovered integer,
#     statecode character varying(20) COLLATE pg_catalog."default",
#     id bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
#     st_district character varying(200) COLLATE pg_catalog."default",
#     CONSTRAINT "st-district" UNIQUE (st_district)
# )
# WITH (
#     OIDS = FALSE
# )
# TABLESPACE pg_default;

# ALTER TABLE public.state_district_wise
#     OWNER to postgres;
