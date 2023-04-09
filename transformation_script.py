import numpy as np
import pandas as pd
from datetime import datetime

dtype_contacts = {
    'facility_uid': str,
    'phone_number': str,
    'alternate_number': str,
    'email_address': str,
    'website': str
}

dtype_identifiers = {
    'facility_uid': str, 'facility_code': str, 'state_unique_id': str,
    'registration_no': str, 'facility_name': str, 'alternate_name': str,
    'start_date': str, 'ownership': str, 'ownership_type': str,
    'facility_level': str, 'facility_level_option': str,
    'days_of_operation': str, 'hours_of_operation': str
}

dtype_locations = {
    'facility_uid': int, 'state': str, 'lga': str,
    'ward': str, 'physical_location': str,
    'postal_address': str, 'longitude': str, 'latitude': str
}

dtype_pages = {
    'state': str, 'lga': str, 'ward': str, 'facility_uid': str,
    'facility_code': str, 'facility_name': str, 'facility_level': str, 'ownership': str
}

dtype_personnel = {
    "facility_uid": str,
    "num_of_docs": np.float64, "num_of_pharms": np.float64,
    "num_of_midwifes": np.float64, "num_of_nurses": np.float64,
    "num_of_nurse_midwife": np.float64, "num_of_pharm_technicians": np.float64,
    "num_of_dentists": np.float64, "num_of_health_attendants": np.float64,
    "num_of_env_health_officers": np.float64, "num_of_him_officers": np.float64,
    "num_of_community_health_officer": np.float64,
    "num_of_jun_community_extension_worker": np.float64,
    "num_of_community_extension_workers": np.float64,
    "num_of_dental_technicians": np.float64,
    "num_of_lab_technicians": np.float64,
    "num_of_lab_scientists": np.float64
}

dtype_services = {
    "facility_uid": str,
    "outpatient_service": str, "ambulance_services": str,
    "mortuary_services": str, "onsite_imaging": str,
    "onsite_pharmarcy": str, "onsite_laboratory": str,
    "tot_num_beds": str, "special_service": str,
    "dental_service": str, "pediatrics_service": str,
    "gynecology_service": str, "surgical_service": str,
    "medical_service": str, "inpatient_service": str
}

dtype_status = {
    "facility_uid": str,
    "operation_status": str,
    "registration_status": str,
    "license_status": str
}


def clean_load_page_rows(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df.fillna("", inplace=True)
    file_df.to_sql(name='page_rows', con=engine, if_exists='append', index=False)


def clean_load_identifiers(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df.start_date.dropna(inplace=True)
    file_df['start_date'] = file_df['start_date'].apply(lambda x: str(x).strip())
    file_df = file_df[file_df['start_date'] != 'nan']  # remove nan rows
    file_df['start_date'] = file_df['start_date'].apply(lambda x: datetime.strptime(x, '%Y-%m-%d'))
    file_df.to_sql(name='identifiers', con=engine, if_exists='append', index=False)


def clean_load_locations(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df.fillna("", inplace=True)
    file_df.drop_duplicates(inplace=True)
    file_df['longitude'] = file_df['longitude'].apply(lambda x: str(x))
    file_df['latitude'] = file_df['latitude'].apply(lambda x: str(x))
    file_df['longitude'] = file_df['longitude'].apply(lambda x: x[:5])
    file_df['latitude'] = file_df['latitude'].apply(lambda x: x[:5])
    file_df['latitude'] = file_df['latitude'].apply(lambda x: '' if ':' in x else x)
    file_df = file_df[file_df['longitude'] != 'nan']
    file_df = file_df[file_df['latitude'] != 'nan']
    file_df = file_df[file_df['latitude'] != '']

    file_df['longitude'] = pd.to_numeric(file_df['longitude'])
    file_df['latitude'] = pd.to_numeric(file_df['latitude'])

    file_df['Region'] = file_df['state'].apply(get_region)
    file_df.to_sql(name='locations', con=engine, if_exists='append', index=False)


def clean_load_contacts(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df.fillna("", inplace=True)
    file_df.drop_duplicates(inplace=True)
    file_df.replace("00__-___-____", 0, inplace=True)
    file_df.to_sql(name='contacts', con=engine, if_exists='append', index=False)


def clean_load_status(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df.fillna("", inplace=True)
    file_df.drop_duplicates(inplace=True)
    file_df.to_sql(name='status', con=engine, if_exists='append', index=False)


def clean_load_services(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df['tot_num_beds'] = pd.to_numeric(file_df['tot_num_beds'])
    file_df['tot_num_beds'].fillna(0)
    file_df.to_sql(name='services', con=engine, if_exists='append', index=False)


def clean_load_personnel(file_path, dtype_dict, engine):
    file_df = pd.read_csv(file_path, usecols=dtype_dict.keys(), dtype=dtype_dict, sep=";")
    file_df.fillna(0, inplace=True)
    file_df = file_df.astype(int)
    file_df.to_sql(name='personnel', con=engine, if_exists='append', index=False)


def get_region(state):
    region = ''
    if state in ["Benue", "FCT", "Kogi", "Kwara", "Nasarawa", "Niger", "Plateau"]:
        region = "North Central"
    elif state in ["Adamawa", "Bauchi", "Borno", "Gombe", "Taraba", "Yobe"]:
        region = "North East"
    elif state in ["Kaduna", "Katsina", "Kano", "Kebbi", "Sokoto", "Jigawa", "Zamfara"]:
        region = "North West"
    elif state in ["Abia", "Anambra", "Ebonyi", "Enugu", "Imo"]:
        region = "South East"
    elif state in ["Akwa Ibom", "Bayelsa", "Cross River", "Delta", "Edo", "Rivers"]:
        region = "South South"
    elif state in ["Ekiti", "Lagos", "Osun", "Ondo", "Ogun", "Oyo"]:
        region = "South West"

    return region
