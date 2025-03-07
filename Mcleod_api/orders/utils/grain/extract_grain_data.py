from ..extract_data_from_pdf import extract_data_from_pdf


def extract_grain_data(file):
    """
    Extracts data from a PDF file and returns a dictionary of the data.
    """
    # Extract data from PDF file
    data = extract_data_from_pdf(file)
    return data
