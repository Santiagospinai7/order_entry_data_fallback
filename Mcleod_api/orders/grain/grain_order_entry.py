import datetime
import json
import logging
import os
import re
import traceback

import pdfplumber
import requests
from config import settings
from orders.client import Client
from orders.order_manager import OrderManager
from requests.auth import HTTPBasicAuth
from utils import pdf_actions

LOGGER = logging.getLogger("orders.grain.grain_order_entry")

class GrainOrderEntry(Client):
    def __init__(self, db):
        super().__init__(db)
        self.customer_id = 'GRAMIA'

    def test_logs(self):
        LOGGER.info('Testing logging from grain_order_entry.')

    def extract_orders(self):
        orders_to_process = []
        elements = os.listdir(f'{settings.GRAIN_ORDERS_PATH}')
        print('extract_orders len:', len(elements))
        pdf_elem_count = 0
        for elem in elements:
            if elem.lower().endswith('.pdf'):
                pdf_elem_count+=1
                
                try:
                    print('add order to orders to process elem', elem)
                    blnum = elem[:-4]  # Assuming elem is a string and you want to get the BOL number.
                    print('bol number extracted from order:', blnum)
                    with pdfplumber.open(f'{settings.GRAIN_ORDERS_PATH}\\{elem}') as pdf:
                        page = pdf.pages[0]
                        text = page.extract_text()
                        table_master = page.extract_table()
                        if not table_master:
                            print(f'No table found in {elem}')
                            continue
                        table1 = str(table_master[0])
                        table = table1.split(r'\n')
                    print('extract_orders orders_to_process', len(orders_to_process))
                    orders_to_process.append((text, table, table_master, elem))
                    print('extract_orders orders_to_process updated', len(orders_to_process))
                except Exception as e:
                    if "No /Root object!" in str(e):
                        print(f"Invalid PDF file {blnum}: {str(e)}")
                        LOGGER.error(f"Invalid PDF file {blnum}: {str(e)}")
                        continue
                    else:
                        print("Another error:", e)
                        raise

        print('total pdf', pdf_elem_count)
        return orders_to_process

    def validate_orders(self, orders):
        for order in orders:
            try:
                print('elem', order)
                ordered_date = datetime.datetime.strptime(order['ordered_date'], '%m/%d/%Y').strftime(
                    '%Y%m%d') + '000000-0600'

                # Check if the order exists in the database
                query = f"SELECT * FROM VTRPA.DBO.{self.table_order_name} WHERE bol = :bol"
                params = {'bol': order['bol']}
                existing_order = self.db.execute_read_query(query, params)

                if not existing_order:
                    print(f"Insert Into db order: {order}")
                    query = f"""
                        INSERT INTO VTRPA.DBO.{self.table_order_name} (
                            bol, cons_ref, cust_order_no, collection_method, customer_id,
                            ordered_date, revenue_code, commodity_desc, commodity, ops_user,
                            equipment_type_id, pickup_addr, pickup_loc_code, pickup_state, cons_addr,
                            cons_loc_code, cons_state, pickup_date, consignee_date, order_status,
                            is_processed, doc_to_attach, status_desc, Processed_date, last_updated
                        ) VALUES (
                            :bol, :cons_ref, :cust_orderno, :collection_method, :cust_id,
                            :ordered_date, :revenue_code, :commodity_desc, :commodity, :ops_user,
                            :equipment_type_id, :pickup_addr, :pickup_loc_code, :pickup_state, :cons_addr,
                            :cons_loc_code, :cons_state, :pickup_date, :cons_date, :order_status,
                            0, :origin_file, :status_desc, :processed_date, :last_updated
                        )
                    """
                    params = {
                        'bol': order['bol'],
                        'cons_ref': order['cons_ref'],
                        'cust_orderno': order['cust_orderno'],
                        'collection_method': order['collection_method'],
                        'cust_id': order['cust_id'],
                        'ordered_date': ordered_date,
                        'revenue_code': order['revenue_code'],
                        'commodity_desc': order['commodity_desc'],
                        'commodity': order['commodity'],
                        'ops_user': order['ops_user'],
                        'equipment_type_id': order['equipment_type_id'],
                        'pickup_addr': '',
                        'pickup_loc_code': '',
                        'pickup_state': '',
                        'cons_addr': '',
                        'cons_loc_code': '',
                        'cons_state': '',
                        'pickup_date': '',
                        'cons_date': '',
                        'order_status': 'flag' if order.get('error') else 'parsed',
                        'origin_file': order['origin_file'],
                        'status_desc': order.get('error') if order.get('error') else 'Parsed',
                        'processed_date': datetime.datetime.now(),
                        'last_updated': datetime.datetime.now(),
                    }
                else:
                    msg = f'order {order["bol"]} already exist in VTRPA'
                    self.existing_orders_in_vtrpa.append(order['bol'])
                    print(msg)
                    LOGGER.info(f"{msg}")

                self.db.execute_write_query(query, params)
                # self.db.commit()

            except Exception as e:
                msg_error = f"Error adding order to database VTRPA: {str(e)}"
                # self.failed_orders.append([order['bol'], msg_error])
                self.failed_orders[order['bol']].append(msg_error)
                order['error'] = msg_error
                LOGGER.error(msg_error)
                # self.db.rollback()  # Rollback for any other unexpected error

        return orders

    def parse_data(self, orders_to_process):
        print(f'to process: {orders_to_process}')
        order_list = []
        for text, table, table_master, file_name in orders_to_process:
            try:
                error = []
                # Extract various details from the text using regular expressions
                ordered_date_match = re.search(r'Date: (\d\d?\/\d\d?\/\d{4})', text)
                ordered_date = ordered_date_match.group(1) if ordered_date_match else ''

                custpo_match = re.search(r'CUSTOMER PO: (\S.*)', text)
                custpo = custpo_match.group(1) if custpo_match else ''

                cust_orderno_match = re.search(r'S\d+', text)
                cust_orderno = cust_orderno_match.group() if cust_orderno_match else ''

                bol_match = re.search(r'\dLID\d+', text)
                bol = bol_match.group() if bol_match else ''

                shipdate_match = re.search(r'\d{2}\/\d\d\/\d{4} ', text)
                shipdate = shipdate_match.group().strip() if shipdate_match else ''

                delidate_match = re.search(r'(\d{2}\/\d\d\/\d{4})\nC', text)
                delidate = delidate_match.group(1) if delidate_match else ''

                # If delivery date is the same as the shipping date, increment delivery date by one day
                if delidate and shipdate and delidate == shipdate:
                    delidate = datetime.datetime.strptime(delidate, '%m/%d/%Y') + datetime.timedelta(days=1)
                    delidate = delidate.strftime('%m/%d/%Y')

                table_text = ' '.join(table)
                table_text_split = table_text.split('\',')

                # Extract pickup and company details from the table
                pickup_match = re.search('PICK UP (.*?) SHIP DATE', table_text_split[0])
                pickup = pickup_match.group(1).replace('.', '') if pickup_match else ''

                print('order pickup', pickup)
                if pickup == '5101 Sevig Street MUSCATINE IA 52761':
                    pickup = '4815 55TH MUSCATINE IA 52761'

                company_match = re.search(r'SHIP TO (\w+)', table_text_split[1])
                company = company_match.group(1) if company_match else ''

                # Extract city, state, and address details
                city_match = re.search(r'((\b[A-Za-z]+(?:\s[A-Za-z]+)?\b)\s([A-Z]{2}))\s(\d{5})',
                                       table_master[0][1].replace('\n', '\n '))
                city = city_match.group(1).strip() if city_match else ''
                state = city.split(' ')[-1]

                modded_table = re.sub(r'C\/O.*?\n', '', table_master[0][1])

                address_match = re.search(f'{city}\n(.*?)\n{city}', modded_table)
                address = address_match.group(1).strip() if address_match and len(
                    address_match.group(1).split(' ')) > 1 else ''
                if not address:
                    address_match = re.search(f'{city}\n(.*?)\n', modded_table)
                    address = address_match.group(1).strip() if address_match and len(
                        address_match.group(1).split(' ')) > 1 else ''
                    if not address:
                        address_match = re.search(f'''{city.split(' ')[1]}.*?\n(.*?)\n''', modded_table)
                        address = address_match.group(1).strip() if address_match else ''
                if company in address:
                    print(modded_table)
                    address_match = re.search(r'(.*?)\n\w+ ?\w+? [A-Z][A-Z] \d{5}', modded_table)
                    address = address_match.group(1).strip() if address_match else ''

                city = city[:-len(state)].strip()

                if '10 MINUTEMAN WAY' in address:
                    msg_error = "CONBMA load, not working currently in API process"
                    LOGGER.error(msg_error)
                    # self.failed_orders.append([bol, msg_error])
                    error.append(msg_error)
                    self.failed_orders[bol].append(msg_error)
                    print()

                    continue

            except Exception as e:
                msg_error = f"Failed to extract information from the blnum: {bol} - {e}"
                LOGGER.error(msg_error)
                # self.failed_orders.append([bol, msg_error])
                self.failed_orders[bol].append(msg_error)
                print(traceback.format_exc())
                return None

            item_list = {
                'bol': bol,
                'cons_ref': custpo,
                'cust_orderno': cust_orderno,
                'collection_method': 'P',
                'cust_id': self.customer_id,
                'ordered_date': ordered_date,
                'revenue_code': 'V',
                'commodity_desc': 'FOOD INGREDIENTS',
                'commodity': 'FOOD-ING',
                'ops_user': 'dbangert',
                'equipment_type_id': 'V',
                'PU_details': [pickup, shipdate],
                'SO_details': [address + ' ' + city + ' ' + state, delidate],
                'so_company': company,
                'origin_file': file_name,
                'error': error[0] if len(error) > 0 else None
            }

            order_list.append(item_list)
        return order_list

    def post_process_orders(self, orders):
        print("Post-processing orders init..")
        try:
            for elem in orders:
                pu_date = elem['PU_details'][1]
                pu_dt = datetime.datetime.strptime(pu_date.strip(), "%m/%d/%Y")
                so_date = elem['SO_details'][1]
                so_dt = datetime.datetime.strptime(so_date.strip(), "%m/%d/%Y")
                pu_dow = datetime.datetime.date(pu_dt).weekday()
                so_dow = datetime.datetime.date(so_dt).weekday()
                week_days = ["monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday"]
                pu_dow_open, pu_dow_close = week_days[pu_dow] + '_open', week_days[pu_dow] + '_close'
                so_dow_open, so_dow_close = week_days[so_dow] + '_open', week_days[so_dow] + '_close'


                pu_addr = elem['PU_details'][0].split(' ')
                so_addr = elem['SO_details'][0].split(' ')

                pu_loc_code_query = f"SELECT [id],[{pu_dow_open}],[{pu_dow_close}] FROM [{self.db.database_name}].[dbo].[location] WHERE is_active = 'Y' and address1 like '%{pu_addr[0]}%' AND state = '{pu_addr[-2]}' AND zip_code='{pu_addr[-1]}' "
                so_loc_code_query = f"SELECT [id],[{so_dow_open}],[{so_dow_close}] FROM [{self.db.database_name}].[dbo].[location] WHERE is_active = 'Y' and address1 like '%{so_addr[0]}%' AND state = '{so_addr[-1]}' AND city_name like '%{so_addr[-2]}%' AND [address1] like '%{so_addr[1]}%' "

                print(f"pu_loc_code_query: {pu_loc_code_query}")
                print(f"so_loc_code_query: {so_loc_code_query}")
                print(f"elements in post process orders {elem}")

                for query in [(pu_loc_code_query, 'PU'), (so_loc_code_query, 'SO')]:
                    print('query element 1 value is:', query[1])

                    try:
                        loc_code = self.db.execute_read_query(query[0])

                        if len(loc_code) == 0:
                            msg_error = f"Location code not found for {query[1]}: {elem[query[1] + '_details'][0]}"
                            # self.failed_orders.append([elem['bol'], msg_error])
                            self.failed_orders[elem['bol']].append(msg_error)
                            elem['error'] = msg_error

                            print(msg_error)
                            LOGGER.error(msg_error)

                        print('loc_code query:', loc_code)
                    except Exception as e:
                        msg_error = f"Error fetching location code: {e}"
                        print(msg_error)
                        LOGGER.error(msg_error)

                    if len(loc_code) == 1:
                        print('loc code 1')
                        elem[query[1] + '_loc_code'] = loc_code[0][0].strip()
                        elem[query[1] + '_open'] = loc_code[0][1].strftime('%H%M') if (
                                    loc_code[0][1] is not None) else '0000'
                        elem[query[1] + '_close'] = loc_code[0][2].strftime('%H%M') if (loc_code[0][2] is not None) else '0000'

                        print(f"elem: {elem}")
                    elif len(loc_code) > 1:
                        try:
                            print('loc code > 1')
                            additional_query = f"{query[0]} AND [name] LIKE :name"
                            params = {'name': f"%{elem['so_company']}%"}
                            loc_code = self.db.execute_read_query(additional_query, params)
                        except Exception as e:
                            msg_error = f"Error fetching location code: {e}"
                            print(msg_error)
                            LOGGER.error(msg_error)

                        if len(loc_code) == 1:
                            elem[query[1] + '_loc_code'] = loc_code[0][0].strip()
                            elem[query[1] + '_open'] = loc_code[0][1].strftime('%H%M')
                            elem[query[1] + '_close'] = loc_code[0][2].strftime('%H%M')
                    else:
                        print('else')
                        if query[1] == 'SO':
                            print('else SO')
                            try:
                                base_query = query[0][:query[0].rfind('AND')]
                                loc_code = self.db.execute_read_query(base_query)
                            except Exception as e:
                                msg_error = f"Error fetching location code: {e}"
                                print(msg_error)
                                LOGGER.error(msg_error)
                            if len(loc_code) == 1:
                                elem[query[1] + '_loc_code'] = loc_code[0][0].strip()
                                elem[query[1] + '_open'] = loc_code[0][1].strftime('%H%M')
                                elem[query[1] + '_close'] = loc_code[0][2].strftime('%H%M')
                            elif len(loc_code) > 1:
                                try:
                                    additional_query = f"{base_query} AND [name] LIKE :name"
                                    params = {'name': f"%{elem['so_company']}%"}
                                    loc_code = self.db.execute_read_query(additional_query, params)
                                except Exception as e:
                                    msg_error = f"Error fetching location code: {e}"
                                    print(msg_error)
                                    LOGGER.error(msg_error)

                                if len(loc_code) == 1:
                                    elem[query[1] + '_loc_code'] = loc_code[0][0].strip()
                                    elem[query[1] + '_open'] = loc_code[0][1].strftime('%H%M')
                                    elem[query[1] + '_close'] = loc_code[0][2].strftime('%H%M')
                        else:
                            if elem['PU_details'][0] == '5101 Sevig Street MUSCATINE IA 52761':
                                elem[query[1] + '_loc_code'] = 'KENMIA'
                                elem[query[1] + '_open'] = '0001'
                                elem[query[1] + '_close'] = '2359'
                if 'SO_loc_code' not in elem.keys():
                    print(f'change SO loc code for {elem["SO_details"][0]}')

                    predefined_addresses = {
                        'US PL FORT WAYNE FORT WAYNE IN': 'EGIFIN',
                        'CUSTOMER PO: 434759 EAST POINT GA': 'BREEGA',
                        'CUSTOMER PO: 24016434 LAUREL MD': 'NESLMD',
                        '445 HURRICANE TRAIL DACULA GA': 'PUBDGA',
                        '1500 SUCKLE HWY PENNSAUKEN TOWNSHIP NJ' : 'BARPNJ'
                    }

                    if elem['SO_details'][0] in predefined_addresses:
                        elem['SO_loc_code'] = predefined_addresses[elem['SO_details'][0]]
                        elem['SO_open'] = '0001'
                        elem['SO_close'] = '2359'

                        # Clear any existing error for this order since we've assigned values manually
                        if 'error' in elem:
                            del elem['error']

                        # Remove from failed orders if it was added earlier
                        if elem['bol'] in self.failed_orders:
                            self.failed_orders.pop(elem['bol'], None)

                        print(f"Predefined values assigned for {elem['SO_details'][0]}")

            print('Orders eln after post processing:', len(orders))
            print('Orders after post processing:', orders)

            for elem in orders:
                elem['PU_open'] = '1201'
                elem['PU_close'] = '2359'
                elem['SO_open'] = '0000'
                elem['SO_close'] = '2359' # validate this
        except Exception as e:
            msg_error = f"Error post processing order blnum: {elem['bol']}, {str(e)}"
            # self.failed_orders.append([elem['bol'], msg_error])
            self.failed_orders[elem['bol']].append(msg_error)
            elem['error'] = msg_error
            LOGGER.error(msg_error)

        return orders

    def update_database(self, orders):
        # Add orders to db
        #      [BOL], [cons_ref], [cust_order_no], [collection_method], [customer_id],
        #       [ordered_date], [revenue_code], [commodity_desc], [commodity], [ops_user],
        #       [equipment_type_id], [pickup_addr], [pickup_city], [pickup_state], [cons_addr],
        #       [cons_city], [cons_state], [pickup_date], [consignee_date], [order_status],
        #       [is_processed], [doc_to_attach]

        for order in orders:
            try:
                print('elem', order)
                ordered_date = datetime.datetime.strptime(order['ordered_date'], '%m/%d/%Y').strftime(
                    '%Y%m%d') + '000000-0600'

                pickup_date = datetime.datetime.strptime(order['PU_details'][1], '%m/%d/%Y').strftime('%Y%m%d')
                pickup_date = pickup_date + order['PU_open'] + '00-0600|' + pickup_date + order['PU_close'] + '00-0600'

                so_date = datetime.datetime.strptime(order['SO_details'][1], '%m/%d/%Y').strftime('%Y%m%d')
                print(f'order: {order}')
                so_date = so_date + order['SO_open'] + '00-0600|' + so_date + order['SO_close'] + '00-0600'
                # so_date = so_date + order['SO_open'] + '00-0600|'

                # if 'SO_close' in order and order['SO_close']:
                #     so_date += so_date + order['SO_close'] + '00-0600'

                query = f"""
                    UPDATE VTRPA.DBO.{self.table_order_name}
                    SET cons_ref = :cons_ref, cust_order_no = :cust_orderno, collection_method = :collection_method,
                        customer_id = :cust_id, revenue_code = :revenue_code,
                        commodity_desc = :commodity_desc, commodity = :commodity, ops_user = :ops_user,
                        equipment_type_id = :equipment_type_id, pickup_addr = :pickup_addr, pickup_loc_code = :pickup_loc_code,
                        pickup_state = :pickup_state, cons_addr = :cons_addr, cons_loc_code = :cons_loc_code,
                        cons_state = :cons_state, pickup_date = :pickup_date, consignee_date = :cons_date, order_status = :order_status,
                        status_desc = :status_desc, last_updated = :last_updated
                    WHERE bol = :bol
                """
                params = {
                    'bol': order['bol'],
                    'cons_ref': order['cons_ref'],
                    'cust_orderno': order['cust_orderno'],
                    'collection_method': order['collection_method'],
                    'cust_id': order['cust_id'],
                    'revenue_code': order['revenue_code'],
                    'commodity_desc': order['commodity_desc'],
                    'commodity': order['commodity'],
                    'ops_user': order['ops_user'],
                    'equipment_type_id': order['equipment_type_id'],
                    'pickup_addr': order['PU_details'][0],
                    'pickup_loc_code': order.get('PU_loc_code', ''),
                    'pickup_state': order['PU_details'][0].split(' ')[-2],
                    'cons_addr': order['SO_details'][0],
                    'cons_loc_code': order.get('SO_loc_code', ''),
                    'cons_state': order['SO_details'][0].split(' ')[-1],
                    'pickup_date': pickup_date,
                    'cons_date': so_date,
                    'order_status': 'flag' if order.get('error') else 'downloaded',
                    'status_desc': order.get('error') if order.get('error') else 'Processed - updated',
                    'last_updated': datetime.datetime.now(),
                }

                print(f"update database for: {order['bol']}, params: {params}")


                self.db.execute_write_query(query, params)
                # self.db.commit()

            except Exception as e:
                msg_error = f"Error adding order to database VTRPA: {str(e)}"
                # self.failed_orders.append([order['bol'], msg_error])
                self.failed_orders[order['bol']].append(msg_error)
                order['error'] = msg_error
                LOGGER.error(msg_error)
                # self.db.rollback()  # Rollback for any other unexpected error
        return

    def post_orders(self):
        order_manager = OrderManager(self.db, self.table_order_name)

        try:
            self.orders_to_post = self.db.execute_read_query(
                f"""
                SELECT * FROM VTRPA.DBO.{self.table_order_name}
                WHERE [order_status] IN ('downloaded') 
                  AND [is_processed] = 0 
                  AND [customer_id] = :customer_id
                """,
                {'customer_id': self.customer_id}
            )
        except Exception as e:
            LOGGER.error(f"Error fetching orders to post: {e}")
            # self.db.rollback()  # Rollback for any other unexpected error
            return

        print("orders to process ...", len(self.orders_to_post))

        # successful_bols = []
        # error_bols = []

        if self.orders_to_post:
            basic_auth = HTTPBasicAuth(self.db.lme_api_user, self.db.lme_api_pw)
            for elem in self.orders_to_post:
                # Check if order with the same bnum already exists in the orders table
                blnum = elem[0]  # Assuming bnum is the first element in the tuple

                # validate blnum is in the failed orders list, not to post
                if blnum in self.failed_orders:
                    continue

                exists_query = f"SELECT id FROM [{self.db.database_name}].[dbo].[orders] WHERE [blnum] = :blnum"
                exists_params = {'blnum': blnum}
                order_exists = self.db.execute_read_query(exists_query, exists_params)

                print('blnum:', blnum, 'order_exists:', order_exists)
                print('elem:', elem)

                if not order_exists:
                    order_successful_post = True
                    order_payload_dict = self.build_order_payload(elem)
                    put_headers = {
                        'Accept': 'application/json', 
                        'Content-Type': 'application/json',
                        'X-com.mcleodsoftware.CompanyID': 'TMS'
                    }

                    print('order_payload (raw):', order_payload_dict)

                    try:
                        created_order = requests.put(self.db.lme_api + '/orders/create', data=order_payload_dict, auth=basic_auth,
                                                headers=put_headers)

                        # if created_order.status_code != 200:
                        #     order_successful_post = False
                        #     LOGGER.error(f'ERROR {elem[0]},api error {created_order.status_code}: {created_order.text}')
                        #     print(
                        #         f"created_order - Could not create order {order_payload_dict['blnum']}, api error {created_order.status_code}: {created_order.text}")
                        #     self.failed_orders.append([order_payload_dict['blnum'],
                        #                                f"API error {created_order.status_code}: {created_order.text}"])
                        #     order_manager.update_order_status(order_payload_dict['blnum'], 'flag',
                        #                                       f"Created Order - API error {created_order.status_code}: {created_order.text}")
                        #     continue
                        if created_order.status_code != 200:
                            try:
                                print("Api create order error")
                                error_message = f"API error {created_order.status_code}: {created_order.text}"
                                LOGGER.error(f"ERROR {elem[0]}, {error_message}")

                                # Ensure order_payload_dict is a dictionary
                                if isinstance(order_payload_dict, str):
                                    order_payload_dict = json.loads(order_payload_dict)

                                # Ensure failed_orders[blnum] is a list
                                self.failed_orders.setdefault(order_payload_dict['blnum'], []).append(error_message)

                                order_manager.update_order_status(order_payload_dict['blnum'], 'flag', error_message)
                            except Exception as e:
                                msg_error = f'Error adding error message to failed_orders list: {e}'
                                LOGGER.error(msg_error)
                            continue

                    # except Exception as e:
                    except requests.exceptions.RequestException as e:
                        print(f"Error creating order via API: {e}")
                        LOGGER.error(f"Error creating order via API: {e}")
                        # self.failed_orders.append([elem[0], f"API error: {e}"])
                        self.failed_orders[elem[0]].append(f"API error: {e}")
                        order_manager.update_order_status(elem[0], 'flag', f"API error: {e}")
                        continue

                    try:
                        order_resp = json.loads(created_order.content)
                        print(f'order_resp: {order_resp}')
                    except json.JSONDecodeError:
                        msg_error = f"Error decoding JSON response from API: {created_order.content}"
                        print(msg_error)
                        LOGGER.error(msg_error)
                        # self.failed_orders.append([elem[0], f"JSONDecodeError: {e}"])
                        self.failed_orders[elem[0]].append(msg_error)

                        continue

                    for e in [(order_resp['shipper_stop_id'], 'PU', order_resp['blnum']), (order_resp['consignee_stop_id'], 'PO', order_resp['consignee_refno'])]:
                        try:
                            query = f"""
                                INSERT INTO {self.db.database_name}.dbo.reference_number (
                                    company_id, element_id, partner_id, reference_number, reference_qual, stop_id, id, 
                                    version, send_to_driver
                                ) VALUES (
                                    'TMS', 128, 'TMS', :reference_number, :reference_qual, :stop_id, 
                                    CONCAT(LOWER(RIGHT(NEWID(), 12)), HOST_NAME()), '004010', 'Y'
                                )
                            """
                            params = {
                                'reference_number': e[2],
                                'reference_qual': e[1],
                                'stop_id': e[0]
                            }

                            self.db.execute_write_query(query, params)

                        except Exception as e:
                            msg_error = f"Error inserting reference number into database: {e}"
                            order_successful_post = False
                            print(msg_error)
                            # self.db.rollback()  # Rollback for any other unexpected error
                            LOGGER.error(msg_error)
                            # self.failed_orders.append([order_resp['blnum'], msg_error])
                            self.failed_orders[order_resp['blnum']].append(msg_error)
                            order_manager.update_order_status(order_resp['blnum'], 'flag', f"Unexpected error: {e}")
                            break

                    if not order_successful_post:
                        continue

                    order_manager.update_order_status(order_resp['blnum'], 'created')

                    # Move the file to the imaging folder
                    # try:
                    print('move file:', f'{settings.GRAIN_ORDERS_PATH}\\' + elem[21])
                    Client.move_file(self, f'{settings.GRAIN_ORDERS_PATH}\\' + elem[21],
                                f'{settings.GRAIN_ORDERS_TO_IMAGING_PATH}')
                    # except Exception:
                    #     LOGGER.error(f'move file error: {settings.GRAIN_ORDERS_PATH + elem[21]}')
                    #     print(f'move file error: {settings.GRAIN_ORDERS_PATH + elem[21]}')

                    try:
                        response_autorate = requests.post(
                            f"{self.db.lme_api}/orders/autorate/{order_resp['id']}",
                            data={'id': order_resp['id']},
                            auth=basic_auth,
                            headers=put_headers
                        )
                    except Exception as e:
                        msg_error = f"Autorate ERROR {elem[0]},api error API connection error: {e}"
                        order_successful_post = False
                        LOGGER.error(msg_error)
                        print(msg_error)
                        # self.failed_orders.append([order_resp['blnum'], msg_error])
                        self.failed_orders[order_resp['blnum']].append(msg_error)
                        continue

                    if response_autorate.status_code != 200:
                        msg_error = f"Autorate ERROR {elem[0]},api error {response_autorate.status_code}: {response_autorate.text}')"
                        order_successful_post = False
                        LOGGER.error(msg_error)
                        print(msg_error)
                        # self.failed_orders.append([order_resp['blnum'], msg_error])
                        self.failed_orders[order_resp['blnum']].append(msg_error)
                        continue

                    print('order_successful_post', order_successful_post)

                    if order_successful_post:
                        order_manager.update_order_status(order_resp['blnum'], 'autorated')
                        
                        try:
                            update_query = f"""
                                UPDATE VTRPA.DBO.{self.table_order_name}
                                SET is_processed = 1
                                WHERE bol = :bol
                                AND is_processed = 0
                            """
                            params = {'bol': order_resp['blnum']}
                            self.db.execute_write_query(update_query, params)
                            print(f"Order {order_resp['blnum']} marked as processed.")
                            self.posted_orders.append(order_resp['blnum'])
                        except Exception as e:
                            msg_error = f"Error updating order status in database: {e}"
                            LOGGER.error(msg_error)
                            print(msg_error)
                            # self.db.rollback()  # Rollback for any other unexpected error
                            # self.failed_orders.append([order_resp['blnum'], msg_error])
                            self.failed_orders[order_resp['blnum']].append(msg_error)
                    else:
                        print(f"Order {order_resp['blnum']} marked as not processed.")

                    print("Response created_order", created_order.status_code)
                    print('Response autorate', response_autorate.status_code)

                    attachment_success = self.handle_attachment(order_resp)
                    if not attachment_success:
                        print(f"Attachment handling failed for order {order_resp['blnum']}")
                else:
                    print(f"Order {blnum} already exists in the orders table.")
                    # validate that the blnum is not in the existing_orders_in_api list
                    if blnum not in self.existing_orders_in_api:
                        self.existing_orders_in_api.append(blnum)

                    continue


                # if os.path.exists(f'{settings.GRAIN_ORDERS_PATH}\\{elem[21]}'):
                #     try:
                #         Client.move_file(self, f'{settings.GRAIN_ORDERS_PATH}' + elem[21],
                #                      f'{settings.GRAIN_ORDERS_TO_IMAGING_PATH}')
                #     except Exception:
                #         print(f"File settings.GRAIN_ORDERS_PATH\\{elem[21]} does not exist.")
                # else:
                #     print(f"File settings.GRAIN_ORDERS_PATH\\{elem[21]} does not exist.")

        print("\nSummary of Order Processing:")
        print("\nSuccessfully posted orders:")
        if self.posted_orders:
            for bol in self.posted_orders:
                print(bol)

        print("\nOrders with errors:")
        for failed_order in self.failed_orders:
            print(f"BOL: {failed_order[0]}, Error: {failed_order[1]}")

        return

    def build_order_payload(self, elem):
        print("__type", "stop", "__name", "stops", "company_id", "TMS",
              "location_id", elem[12],
              "sched_arrive_early", elem[17].split('|')[0],
              "sched_arrive_late", elem[17].split('|')[1],
              "stop_type", "PU"
                           "__type", "stop",
              "__name", "stops",
              "company_id", "TMS",
              "location_id", elem[15],
              "sched_arrive_early", elem[18].split('|')[0],
              "sched_arrive_late", elem[18].split('|')[1],
              "stop_type", "SO")

        order_template = {"__type": "orders",
                          "company_id": "TMS",
                          "blnum": elem[0],
                          "consignee_refno": elem[1].strip(),
                          "cust_order_no": elem[2].strip(),
                          "collection_method": elem[3].strip(),
                          "customer_id": elem[4].strip(),
                          "ordered_date": elem[5],
                          "ordered_method": "M",
                          "revenue_code_id": elem[6].strip(),
                          "commodity_id": elem[8].strip(),
                          "commodity": elem[7].strip(),
                          "operations_user": elem[9].strip(),
                          "equipment_type_id": elem[10].strip(),
                          "stops": [
                              {
                                  "__type": "stop",
                                  "__name": "stops",
                                  "company_id": "TMS",
                                  "location_id": elem[12],
                                  "sched_arrive_early": elem[17].split('|')[0],
                                  "sched_arrive_late": elem[17].split('|')[1],
                                  "stop_type": "PU"
                              },
                              {
                                  "__type": "stop",
                                  "__name": "stops",
                                  "company_id": "TMS",
                                  "location_id": elem[15],
                                  "sched_arrive_early": elem[18].split('|')[0],
                                  "sched_arrive_late": elem[18].split('|')[1],
                                  "stop_type": "SO"
                              }
                          ]
                          }
        
        if elem[4] == self.customer_id:
            order_template['stops'][1].pop('sched_arrive_late')
        return json.dumps(order_template)

    def handle_attachment(self, order_resp):
        if order_resp['customer_id'] == self.customer_id:
            result = self.attach_bol(order_resp)
        else:
            result = False

        return result

    def attach_bol(self, order_resp):
        base_path = os.getcwd() + '\\documents\\order_entry\\grain\\'

        # Find the PDF file
        bol_file = pdf_actions.find_pdf_file(base_path, order_resp['blnum'])

        if bol_file is None:
            print(f"Error: No matching PDF file found for BOL {order_resp['blnum']}")
            return False

        bol_file_path = os.path.join(base_path, bol_file)

        # Define the path for images and create the directory if it doesn't exist
        image_dir = os.path.join(base_path, 'images', order_resp['blnum'])
        os.makedirs(image_dir, exist_ok=True)

        # Define the image path
        image_path = os.path.join(image_dir, '')
        print('Attached bol file path', bol_file_path)

        try:
            images_to_attach = pdf_actions.convert_pdf_image(bol_file_path, image_path)
            return True
        except Exception as e:
            print(f"Error converting PDF to images: {e}")
            return False