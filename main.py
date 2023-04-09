"""
The default mode of the program is to run in test mode, when it does this, it'll scrape the data on 10 pages and display
the web browser. When the goal is to scrape all the data on the ministry of health webpage pass in the argument value of
'False' to the class instance, this will scrape all the data and hide the web browser display
"""

import transformation_script as ts
from extraction import MoHScrapper
import pandas as pd
from sqlalchemy import create_engine

# DEFINE THE DATABASE CREDENTIALS
# change these values to your personal db params
user = 'edu_user'
password = 'edungwo1'
host = '127.0.0.1'
port = 5432
database = 'analysis'


# PYTHON FUNCTION TO CONNECT TO THE POSTGRESQL DATABASE AND
# RETURN THE SQLALCHEMY ENGINE OBJECT
def get_connection():
    return create_engine(
        url="postgresql://{0}:{1}@{2}:{3}/{4}".format(
            user, password, host, port, database
        )
    )


def main():
    # the db engine would be used to insert data into a postgresdb
    engine = get_connection()

    website = "https://hfr.health.gov.ng/facilities/hospitals-search?_token=uHw9x4DLz9c8MwyEEHT7icRzqQ58EbDYmDotb9Ez" \
              "&state_id=1&ward_id=0&facility_level_id=0&ownership_id=0&operational_status_id=1" \
              "&registration_status_id=2&license_status_id=1&geo_codes=0&service_type=0&service_category_id=0" \
              "&entries_per_page=20&page=1"
    data = MoHScrapper(website, test=True)
    result = data.scrape_mh_data()
    file_path = '/Users/Chinedu/Documents/'

    for key in result:
        data_table_df = pd.DataFrame.from_records(result[key])
        data_table_df.to_csv(file_path + f"{key}.csv", index=False, sep=";")  # save extracted data

        if key == "page_rows":
            ts.clean_load_page_rows(file_path + f"{key}.csv", ts.dtype_pages, engine)
        elif key == "identifiers":
            ts.clean_load_identifiers(file_path + f"{key}.csv", ts.dtype_identifiers, engine)
        elif key == "locations":
            ts.clean_load_locations(file_path + f"{key}.csv", ts.dtype_locations, engine)
        elif key == "contacts":
            ts.clean_load_contacts(file_path + f"{key}.csv", ts.dtype_contacts, engine)
        elif key == "status":
            ts.clean_load_status(file_path + f"{key}.csv", ts.dtype_status, engine)
        elif key == "services":
            ts.clean_load_services(file_path + f"{key}.csv", ts.dtype_services, engine)
        elif key == "personnel":
            ts.clean_load_personnel(file_path + f"{key}.csv", ts.dtype_personnel, engine)


# Run the script.
if __name__ == '__main__':
    main()
