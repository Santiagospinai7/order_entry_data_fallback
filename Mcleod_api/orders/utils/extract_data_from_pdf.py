
from .grain import extract_grain_data


def extract_data_from_pdf(process="grain", file=None):
    """
    Extracts data from a PDF file and returns a dictionary of the data.
    """
    if process == "grain":
        return extract_grain_data(file)
    elif process == "resolute_inbound":
        pass
        # return extract_resolute_inbound_data(file)
    elif process == "resolute_outbound":
        pass
        # return extract_resolute_outbound_data(file)
    else:
        raise ValueError(f"Invalid process type: {process}")
    
