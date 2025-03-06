import logging

from orders.grain import GrainOrderEntry, process_grain_orders
from orders.resolute import (
    ResoluteInbound,
    ResoluteOutbound,
    process_resolute_inbound_orders,
    process_resolute_outbound_orders,
)

# Get the specific logger for this module
LOGGER = logging.getLogger("orders.order_entry_master")

async def process_orders(db, order_types):
    results = []

    try:
        # Check which order types to process
        if "grain" in order_types or "all" in order_types:
            try:
                grain_result = await process_grain_orders(db)
                print('grain result', grain_result)
                results.append(grain_result)
            except Exception as e:
                msg_error = f"Error processing grain orders: {e}"
                LOGGER.error(msg_error)
                print(msg_error)
                raise ValueError(msg_error)

        if "resolute_inbound" in order_types or "all" in order_types:
            try:
                resolute_inbound_result = await process_resolute_inbound_orders(db)
                if resolute_inbound_result is None:
                    raise ValueError("No orders were processed for resolute inbound.")
                results.append(resolute_inbound_result)
            except Exception as e:
                msg_error = f"Error processing resolute inbound orders: {e}"
                LOGGER.error(msg_error)
                print(msg_error)
                raise ValueError(msg_error)

        if "resolute_outbound" in order_types or "all" in order_types:
            try:
                resolute_outbound_result = await process_resolute_outbound_orders(db)
                if resolute_outbound_result is None:
                    raise ValueError("No orders were processed, received None as result.")

                results.append(resolute_outbound_result)
            except Exception as e:
                msg_error = f"Error processing resolute outbound orders: {e}"
                LOGGER.error(msg_error)
                print(msg_error)
                raise ValueError(msg_error)
    
    except ValueError as e:
        LOGGER.error(f"Error in processing orders: {e}")
        results.append(('Error', str(e)))
    except Exception as e:
        LOGGER.error(f"Unexpected error in process_orders: {e}")
        results.append(('Error', 'An unexpected error occurred during processing'))

    if len(results) == 0:
        raise ValueError("No orders were processed, received None as result.")

    # update_report(db, results)  # Update the report based on the result
    print('Results:', results)
    return results

def update_report(db, result):
    try:
        if result[1] == 1:
            query = 'UPDATE VTUtility.dbo.Status_Script SET result = 1, last_execution = GETDATE() WHERE id = 16'
            db.execute_write_query(query)
        else:
            query = 'UPDATE VTUtility.dbo.Status_Script SET result = 0, last_execution = GETDATE(), comments = :comments'
            params = {'comments': result[2]}
            db.execute_write_query(query, params)
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def test_logs():
    try:
        LOGGER.info('Testing logging from order_entry_master.')
        grain = GrainOrderEntry(None)
        resolute_inbound = ResoluteInbound(None)
        resolute_outbound = ResoluteOutbound(None)

        grain.test_logs()
        resolute_inbound.test_logs()
        resolute_outbound.test_logs()
        return 'Logs tested successfully.'
    except Exception as e:
        LOGGER.error(f"Exception in test_logs: {e}")
        raise ValueError(f"Exception: {e}")
