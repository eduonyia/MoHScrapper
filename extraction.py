from typing import List, Dict, Any
from log import load_logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.chrome.options import Options


class MoHScrapper:
    def __init__(self, health_data_website, test=True):
        # load logger
        self.logger = load_logging(__name__)

        # create empty list to store an array of dicts for each table
        self.page_rows = []
        self.identifiers = []
        self.locations = []
        self.contacts = []
        self.status = []
        self.services = []
        self.personnel = []

        self.page = 1  # current_page
        self.test = test

        if self.test is True:
            try:

                self.logger.info(f"Ministry of Health data scrapping has started...")

                self.driver = webdriver.Chrome()
                self.driver.get(health_data_website)

            except Exception:
                self.logger.error("An error occurred opening the Ministry of Health webpage", exc_info=True)

        else:
            try:
                self.logger.info(f"Ministry of Health data scrapping has started...")

                options = Options()
                options.headless = True

                self.driver = webdriver.Chrome(options=options)
                self.driver.get(health_data_website)

            except Exception:
                self.logger.error("An error occurred opening the Ministry of Health webpage", exc_info=True)

    def scrape_mh_data(self) -> Dict[str, List[Any]]:
        while True:
            page_table = self.get_page_table()
            page_body = self.get_table_body(page_table)

            # get all view button along with their associated attributes
            view_buttons = self.get_page_view_buttons(page_body)

            self.extract_view_buttons_data(view_buttons)

            if self.test is True:  # for testing purposes only scrapes 5 pages worth of data
                if self.page == 2:
                    break

            page_bool = self.get_next_page()

            if page_bool is False:
                break

        full_data = {
            "page_rows": self.page_rows, "identifiers": self.identifiers, "locations": self.locations,
            "contacts": self.contacts, "status": self.status, "services": self.services, "personnel": self.personnel
        }

        return full_data

    def get_page_table(self) -> WebElement | None:
        try:

            page_table = self.driver.find_element(By.ID, "hosp")
            self.logger.info(f"getting table tag element for page {self.page}")
            return page_table

        except Exception:
            self.logger.error(f"An error occurred getting table tag element for page {self.page}", exc_info=True)
            return None

    @staticmethod
    def get_table_body(page_table: WebElement) -> WebElement:
        body = page_table.find_element(By.TAG_NAME, "tbody")  # find body tag
        return body

    @staticmethod
    def get_page_view_buttons(table_body: WebElement) -> list[WebElement]:
        buttons = table_body.find_elements(By.TAG_NAME, "button")
        return buttons

    def extract_view_buttons_data(self, buttons: list[WebElement]) -> None:
        row_num = 1
        for button in buttons:
            try:
                self.logger.info(f"page:{self.page}, row:{row_num}")
                self.extract_data(button)
                row_num += 1

            except Exception:
                self.logger.error(f"issue extracting data for page:{self.page}, row:{row_num}")

    def extract_data(self, button_elem: WebElement):
        self.get_page_rows(button_elem)
        self.get_identifiers(button_elem)
        self.get_location(button_elem)
        self.get_contacts(button_elem)
        self.get_status(button_elem)
        self.get_services(button_elem)
        self.get_personnel(button_elem)

    def get_next_page(self) -> bool:
        pagination = self.driver.find_element(By.CLASS_NAME, "pagination")
        try:
            next_page = pagination.find_element(By.LINK_TEXT, '›')

            next_page.click()
            self.page += 1
            return True
        except NoSuchElementException:
            self.logger.info("End of Health Ministry data pages")
            return False

    def get_identifiers(self, button_elem: WebElement) -> None:
        facility_uid = button_elem.get_attribute("data-id")
        facility_code = button_elem.get_attribute("data-unique_id")
        state_unique_id = button_elem.get_attribute("data-state_unique_id")
        registration_no = button_elem.get_attribute("data-registration_no")
        facility_name = button_elem.get_attribute("data-facility_name")
        alternate_name = button_elem.get_attribute("data-alt_facility_name")
        start_date = button_elem.get_attribute("data-start_date")
        ownership = button_elem.get_attribute("data-ownership")
        ownership_type = button_elem.get_attribute("data-ownership_type")
        facility_level = button_elem.get_attribute("data-facility_level")
        facility_level_option = button_elem.get_attribute("data-facility_level_option")
        days_of_operation = button_elem.get_attribute("data-operational_days")
        hours_of_operation = button_elem.get_attribute("data-operational_hours")

        data = {
            "facility_uid": facility_uid,
            "facility_code": facility_code, "state_unique_id": state_unique_id,
            "registration_no": registration_no, "facility_name": facility_name,
            "alternate_name": alternate_name, "start_date": start_date,
            "ownership": ownership, "ownership_type": ownership_type,
            "facility_level": facility_level, "facility_level_option": facility_level_option,
            "days_of_operation": days_of_operation, "hours_of_operation": hours_of_operation
        }
        self.identifiers.append(data)

    def get_location(self, button_elem: WebElement) -> None:
        facility_uid = button_elem.get_attribute("data-id")
        state = button_elem.get_attribute("data-state")
        lga = button_elem.get_attribute("data-lga")
        ward = button_elem.get_attribute("data-ward")
        physical_location = button_elem.get_attribute("data-physical_location")
        postal_address = button_elem.get_attribute("data-postal_address")
        longitude = button_elem.get_attribute("data-longitude")
        latitude = button_elem.get_attribute("data-latitude")

        data = {
            "facility_uid": facility_uid, "state": state, "lga": lga,
            "ward": ward, "physical_location": physical_location,
            "postal_address": postal_address, "longitude": longitude, "latitude": latitude
        }

        self.locations.append(data)

    def get_contacts(self, button_elem: WebElement) -> None:
        facility_uid = button_elem.get_attribute("data-id")
        phone_number = button_elem.get_attribute("data-phone_number")
        alternate_number = button_elem.get_attribute("data-alternate_number")
        email_address = button_elem.get_attribute("data-email_address")
        website = button_elem.get_attribute("data-website")

        data = {
            "facility_uid": facility_uid,
            "phone_number": phone_number, "alternate_number": alternate_number,
            "email_address": email_address, "website": website
        }

        self.contacts.append(data)

    def get_status(self, button_elem: WebElement) -> None:
        facility_uid = button_elem.get_attribute("data-id")
        operation_status = button_elem.get_attribute("data-operation_status")
        registration_status = button_elem.get_attribute("data-registration_status")
        license_status = button_elem.get_attribute("data-license_status")

        data = {
            "facility_uid": facility_uid,
            "operation_status": operation_status,
            "registration_status": registration_status,
            "license_status": license_status
        }

        self.status.append(data)

    def get_services(self, button_elem):
        facility_uid = button_elem.get_attribute("data-id")
        outpatient_service = button_elem.get_attribute('data-outpatient')
        inpatient_service = button_elem.get_attribute('data-inpatient')
        medical_service = button_elem.get_attribute('data-medical')
        surgical_service = button_elem.get_attribute('data-surgical')
        gynecology_service = button_elem.get_attribute('data-gyn')
        pediatrics_service = button_elem.get_attribute('data-pediatrics')
        dental_service = button_elem.get_attribute('data-dental')
        special_service = button_elem.get_attribute('data-specialservice')
        tot_num_beds = button_elem.get_attribute('data-beds')
        onsite_laboratory = button_elem.get_attribute('data-onsite_laboratory')
        onsite_imaging = button_elem.get_attribute('data-onsite_imaging')
        onsite_pharmarcy = button_elem.get_attribute('data-onsite_pharmarcy')
        mortuary_services = button_elem.get_attribute('data-mortuary_services')
        ambulance_services = button_elem.get_attribute('data-ambulance_services')

        data = {"facility_uid": facility_uid,
                "outpatient_service": outpatient_service, "ambulance_services": ambulance_services,
                "mortuary_services": mortuary_services, "onsite_imaging": onsite_imaging,
                "onsite_pharmarcy": onsite_pharmarcy, "onsite_laboratory": onsite_laboratory,
                "tot_num_beds": tot_num_beds, "special_service": special_service,
                "dental_service": dental_service, "pediatrics_service": pediatrics_service,
                "gynecology_service": gynecology_service, "surgical_service": surgical_service,
                "medical_service": medical_service, "inpatient_service": inpatient_service}

        self.services.append(data)

    def get_personnel(self, button_elem):
        facility_uid = button_elem.get_attribute("data-id")
        num_of_docs = button_elem.get_attribute('data-doctors')
        num_of_pharms = button_elem.get_attribute('data-pharmacists')
        num_of_pharm_technicians = button_elem.get_attribute('data-pharmacy_technicians')
        num_of_dentists = button_elem.get_attribute('data-dentist')
        num_of_dental_technicians = button_elem.get_attribute('data-dental_technicians')
        num_of_nurses = button_elem.get_attribute('data-nurses')
        num_of_midwifes = button_elem.get_attribute('data-midwifes')
        num_of_nurse_midwife = button_elem.get_attribute('data-nurse_midwife')
        num_of_lab_technicians = button_elem.get_attribute('data-lab_technicians')
        num_of_lab_scientists = button_elem.get_attribute('data-lab_scientists')
        num_of_him_officers = button_elem.get_attribute('data-him_officers')
        num_of_community_health_officer = button_elem.get_attribute('data-community_health_officer')
        num_of_community_extension_workers = button_elem.get_attribute('data-community_extension_workers')
        num_of_jun_community_extension_worker = button_elem.get_attribute('data-jun_community_extension_worker')
        num_of_env_health_officers = button_elem.get_attribute('data-env_health_officers')
        num_of_health_attendants = button_elem.get_attribute('data-attendants')

        data = {"facility_uid": facility_uid,
                "num_of_docs": num_of_docs, "num_of_pharms": num_of_pharms,
                "num_of_midwifes": num_of_midwifes, "num_of_nurses": num_of_nurses,
                "num_of_nurse_midwife": num_of_nurse_midwife, "num_of_pharm_technicians": num_of_pharm_technicians,
                "num_of_dentists": num_of_dentists, "num_of_health_attendants": num_of_health_attendants,
                "num_of_env_health_officers": num_of_env_health_officers, "num_of_him_officers": num_of_him_officers,
                "num_of_community_health_officer": num_of_community_health_officer,
                "num_of_jun_community_extension_worker": num_of_jun_community_extension_worker,
                "num_of_community_extension_workers": num_of_community_extension_workers,
                "num_of_dental_technicians": num_of_dental_technicians,
                "num_of_lab_technicians": num_of_lab_technicians,
                "num_of_lab_scientists": num_of_lab_scientists}

        self.personnel.append(data)

    def get_page_rows(self, button_elem):
        state = button_elem.get_attribute("data-state")
        lga = button_elem.get_attribute("data-lga")
        ward = button_elem.get_attribute("data-ward")
        facility_uid = button_elem.get_attribute("data-id")
        facility_code = button_elem.get_attribute("data-unique_id")
        facility_name = button_elem.get_attribute("data-facility_name")
        facility_level = button_elem.get_attribute("data-facility_level")
        ownership = button_elem.get_attribute("data-ownership")

        data = {
            "state": state, "lga": lga, "ward": ward, "facility_uid": facility_uid,
            "facility_code": facility_code, "facility_name": facility_name, "facility_level": facility_level,
            "ownership": ownership
        }

        self.page_rows.append(data)