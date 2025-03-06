import logging
import logging.config

# Now import modules that use loggers
import orders.order_entry_master as order_entry_master
from database import DatabaseHandler  # New import
from fastapi import FastAPI, HTTPException, Request  # Import Request here
from logging_config import setup_logging

# Setup logging
setup_logging()

# Get loggers
app_logger = logging.getLogger("app")

# Set the desired log level
# logging.basicConfig(level=logging.INFO)

# Initialize FastAPI app
app = FastAPI()

# Suppress SQLAlchemy query logs by setting the logging level to WARNING
# logging.getLogger('sqlalchemy.engine').setLevel(logging.WARNING)

# Initialize the DatabaseHandler
db_handler = DatabaseHandler()

VALID_ORDER_TYPES = ["grain", "resolute_inbound", "resolute_outbound"]

@app.post("/process_orders")
async def process_orders(request: Request):
    try:
        # Get the order_type parameter from the query, default to "all" if not provided
        order_type = request.query_params.get("order_type", "all").split(",")  # Support multiple types

        # Validate that each order_type is valid
        for ot in order_type:
            if ot != "all" and ot not in VALID_ORDER_TYPES:
                valid_options = ", ".join(VALID_ORDER_TYPES)
                raise HTTPException(
                    status_code=400,
                    detail=f"Invalid order_type: '{ot}'. Valid options are: {valid_options}."
                )
        
        print("Session created:", db_handler.is_healthy(), "in the env", db_handler.env)
        
        # Process orders based on the provided order_type
        result = await order_entry_master.process_orders(db_handler, order_type)  # Pass the session to process_orders
        app_logger.info("Process orders endpoint called.")
        return result
    
    except HTTPException as e:
        # If we raised an HTTPException (for bad request)
        app_logger.error(f"Bad request error: {e.detail}")
        raise e  # Re-raise the exception for FastAPI to handle it as a 400 error
    
    except Exception as e:
        # Catch any other exception and log it
        app_logger.error(f"Exception in process_orders: {e}")
        raise HTTPException(status_code=500, detail=f"Exception: {e}")

