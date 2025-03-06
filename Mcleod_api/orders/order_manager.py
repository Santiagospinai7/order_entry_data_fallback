# order_manager.py
import logging

LOGGER = logging.getLogger("orders.grain.order_entry_master")

class OrderManager:
    def __init__(self, db, table_order_name):
        self.db = db
        self.table_order_name = table_order_name

    def update_order_status(self, bol, new_status, status_desc=None):
        """
        Updates the status of an order in the database.

        :param bol: BOL number of the order.
        :param new_status: New status of the order.
        :param status_desc: Description of the status, if applicable.
        """
        query = f"""
                UPDATE VTRPA.DBO.{self.table_order_name}
                SET order_status = :new_status{', status_desc = :status_desc' if status_desc else ''}
                WHERE bol = :bol
            """
        params = {'new_status': new_status, 'bol': bol}
        if status_desc:
            params['status_desc'] = status_desc

        try:
            self.db.execute_write_query(query, params)
        except Exception as e:
            msg_desc = f" with status_desc: {status_desc}" if status_desc else ''
            LOGGER.error(f"Error updating order status for BOL: {bol} to {new_status}{msg_desc}: {e}")
            print(f"Error updating order status for BOL: {bol} to {new_status}{msg_desc}: {e}")
            raise e
