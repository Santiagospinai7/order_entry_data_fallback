from orders.grain.grain_order_entry import GrainOrderEntry


async def process_grain_orders(db):
    try:
        grain = GrainOrderEntry(db)
        grain_result = grain.process_orders()

        if grain_result is None:
            raise ValueError("No orders were processed, received None as result.")
        else:
            return grain_result
    except Exception as e:
        raise ValueError(f"Exception in process_grain_orders: {e}")