import logging
import os
import shutil
from abc import ABC, abstractmethod
from collections import defaultdict

from dotenv import load_dotenv

LOGGER = logging.getLogger("orders.order_entry_master")

class Client(ABC):
    def __init__(self, db):
        load_dotenv('config.env')
        self.db = db
        self.files_to_process = [] # Files that will be processed
        self.orders_to_insert_into_db = [] # Orders that will be inserted into VTRPA SQL DB DONE
        self.voided_orders = [] # Orders that were voided
        self.existing_orders_in_vtrpa = [] # Orders that already exist in VTRPA SQL DB or had been parsed
        self.existing_orders_in_api = [] # Orders that already exist in lme API DONE
        self.orders_to_post = [] # Orders that will be posted to the API DONE
        self.failed_orders = defaultdict(list) # Orders that failed to be posted to the API or inserted into the VTRPA SQL DB or parsed DONE
        self.posted_orders = [] # Orders that were successfully posted to the API DONE
        self.table_order_name = os.getenv('TABLE_ORDERS_PROD') if os.getenv('ENV') == 'production' else os.getenv('TABLE_ORDERS_DEV')

    def process_orders(self):
        try:
            self.files_to_process = self.extract_orders()
            print('Files to process len:', len(self.files_to_process))
            print('Files to process:', self.files_to_process)
        except Exception as e:
            raise Exception(f"Error extracting orders: {str(e)}")

        if self.files_to_process:
            try:
                self.orders_to_insert_into_db = self.parse_data(self.files_to_process)
                print('Parsed orders len:', len(self.orders_to_insert_into_db))
            except Exception as e:
                raise Exception(f"Error parsing orders: {str(e)}")

            try:
                self.orders_to_insert_into_db = self.validate_orders(self.orders_to_insert_into_db)
                print('Valid orders len:', len(self.orders_to_insert_into_db))
            except Exception as e:
                raise Exception(f"Error validating orders: {str(e)}")

            try:
                self.orders_to_insert_into_db = self.post_process_orders(self.orders_to_insert_into_db)
                print("Orders after post processing len:", len(self.orders_to_insert_into_db))
            except Exception as e:
                raise Exception(f"Error post processing orders: {str(e)}")

            try:
                self.update_database(self.orders_to_insert_into_db)
                LOGGER.info('Successfully added orders to be processed into VTRPA SQL DB.')
            except Exception as e:
                raise Exception(f"Error updating database: {str(e)}")

        try:
            self.post_orders()
        except Exception as e:
            LOGGER.error(f"Error posting orders: {str(e)}")
            raise Exception(f"Error posting orders: {str(e)}")
        #
        # if self.failed_orders:
        #     try:
        #         message = "An error occurred while processing the following orders."
        #         #send_email(subject="Failed orders in the process", message=message, failed_orders=self.failed_orders)
        #     except Exception as e:
        #         raise Exception(f"Error sending email: {str(e)}")


        try:
            print('Total orders received:', len(self.files_to_process))
            print('Orders already processed:', len(self.existing_orders_in_vtrpa))
            print('Existing orders in API:', len(self.existing_orders_in_api))
            print('Successful API posts:', len(self.posted_orders))
            print('Failed orders:', len(self.failed_orders))
            total_orders_in_folder = len(self.existing_orders_in_api) + len(
                self.posted_orders) + len(self.failed_orders)

            print("Final Results")
            print(f'total_orders_received: {total_orders_in_folder}')
            print(f"files_to_process: {self.files_to_process}")
            print(f"orders_already_processed: {self.existing_orders_in_vtrpa}")
            print(f"existing_orders_in_api: {self.existing_orders_in_api}")
            print(f"successful_api_posts: {self.posted_orders}")
            print(f"failed_orders: {[[bol, errors] for bol, errors in self.failed_orders.items()]}")

            print(f"successful_api_posts list: {self.posted_orders}")
        except Exception as e:
            LOGGER.error(f"Error generating final summary {e}")
            raise Exception(f"Error generating final summary {e}")
        """
        return {
            "files_to_process": len(self.files_to_process),
            "orders_to_insert_into_db": len(self.orders_to_insert_into_db),
            "failed_orders": self.failed_orders,
        }
        """

        # return {
        #     "total_orders_received": len(self.files_to_process),
        #     "orders_already_processed": len(self.existing_orders_in_vtrpa),
        #     "existing_orders_in_api": len(self.existing_orders_in_api),
        #     "successful_api_posts": len(self.posted_orders),
        #     "failed_orders": [[bol, errors] for bol, errors in self.failed_orders.items()],
        # }
        return f"Total orders in folder: {len(self.files_to_process)}, Total orders handled (orders without process and in downloaded state): {total_orders_in_folder}, Files processed (parsed): {len(self.files_to_process)}, Orders already processed (parsed): {len(self.existing_orders_in_vtrpa)}, Existing orders in LME API: {len(self.existing_orders_in_api)}, Successful LME API posts: {len(self.posted_orders)}, Failed orders: {len(self.failed_orders)}"

        ##Add email message that will fwd errored out orders and their error message to CS team and myself.
        ##Review status description update in order_entry_data table.

    def move_file(self, file_path, destination_path):
        try:
            if os.path.exists(file_path):
                shutil.move(file_path, destination_path)
                LOGGER.info(f"Successfully moved file from {file_path} to {destination_path}")
            else:
                LOGGER.error(f"File not found: {file_path}")
        except Exception as e:
            LOGGER.error(f"Error moving file: {str(e)}")

    @abstractmethod
    def test_logs(self):
        pass

    @abstractmethod
    def extract_orders(self):
        pass

    @abstractmethod
    def parse_data(self, orders):
        pass

    @abstractmethod
    def validate_orders(self, orders):
        pass

    @abstractmethod
    def post_process_orders(self, orders):
        pass

    @abstractmethod
    def post_orders(self):
        pass

    @abstractmethod
    def build_order_payload(self, elem):
        pass

    @abstractmethod
    def handle_attachment(self, order_resp):
        pass

    @abstractmethod
    def attach_bol(self, order_resp):
        pass
