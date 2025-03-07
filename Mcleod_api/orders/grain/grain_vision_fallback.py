# orders/grain/grain_vision_fallback.py
import io
import logging
import os
import re

from google.cloud import vision
from pdf2image import convert_from_path

# Get the specific logger for this module
LOGGER = logging.getLogger("orders.grain.grain_vision_fallback")

class GrainVisionFallback:
    def __init__(self, temp_dir='temp_images'):
        """Initialize the Vision API fallback system"""
        self.client = vision.ImageAnnotatorClient()
        self.temp_dir = temp_dir
        # Create temp directory if it doesn't exist
        os.makedirs(self.temp_dir, exist_ok=True)
        
    def extract_location_from_pdf(self, pdf_path):
        """Extract location information from the first page of a PDF using Google Vision API"""
        try:
            # Convert first page of PDF to image
            images = convert_from_path(pdf_path, first_page=1, last_page=1)
            if not images:
                LOGGER.error(f"Failed to convert PDF to image: {pdf_path}")
                return None
                
            # Save image temporarily
            img_path = os.path.join(self.temp_dir, f"{os.path.basename(pdf_path)}_page1.jpg")
            images[0].save(img_path, 'JPEG')
            
            # Process the image with Vision API
            with io.open(img_path, 'rb') as image_file:
                content = image_file.read()
            
            image = vision.Image(content=content)
            response = self.client.document_text_detection(image=image)
            
            if not response.text_annotations:
                LOGGER.error(f"No text detected in the image: {img_path}")
                return None
                
            # Extract the full text
            document_text = response.full_text_annotation.text
            
            # Clean up temporary file
            os.remove(img_path)
            
            # Process the extracted text to find location information
            location_info = self._parse_location_info(document_text)
            
            return location_info
            
        except Exception as e:
            LOGGER.error(f"Error extracting location from PDF: {e}")
            return None
            
    def _parse_location_info(self, text):
        """Parse location information from the extracted text"""
        location_info = {
            'PU': {'address': None, 'city': None, 'state': None, 'zip_code': None},
            'SO': {'address': None, 'city': None, 'state': None, 'zip_code': None}
        }
        
        # Extract pickup location (pattern matching)
        pickup_match = re.search(r'PICK UP\s+(.*?)(?=SHIP DATE|SHIP TO|$)', text, re.DOTALL)
        if pickup_match:
            pickup_text = pickup_match.group(1).strip()
            # Parse address components
            pu_components = self._parse_address_components(pickup_text)
            location_info['PU'].update(pu_components)
        
        # Extract ship to location (pattern matching)
        shipto_match = re.search(r'SHIP TO\s+(.*?)(?=CARRIER|NOTES|$)', text, re.DOTALL)
        if shipto_match:
            shipto_text = shipto_match.group(1).strip()
            # Parse address components
            so_components = self._parse_address_components(shipto_text)
            location_info['SO'].update(so_components)
            
        return location_info
    
    def _parse_address_components(self, address_text):
        """Parse address components from an address string"""
        components = {'address': None, 'city': None, 'state': None, 'zip_code': None}
        
        # Try to match US address pattern: CITY STATE ZIP
        city_state_zip = re.search(r'([A-Za-z\s\.]+)\s+([A-Z]{2})\s+(\d{5}(?:-\d{4})?)', address_text)
        if city_state_zip:
            components['city'] = city_state_zip.group(1).strip()
            components['state'] = city_state_zip.group(2)
            components['zip_code'] = city_state_zip.group(3)
            
            # Everything before city/state/zip is the address
            addr_end_pos = address_text.find(city_state_zip.group(0))
            if addr_end_pos > 0:
                # Get address lines before city/state/zip
                components['address'] = address_text[:addr_end_pos].strip()
        else:
            # If standard pattern doesn't match, try to extract state code
            state_match = re.search(r'\s([A-Z]{2})\s', address_text)
            if state_match:
                components['state'] = state_match.group(1)
                
                # Try to extract city before state
                parts = address_text.split(state_match.group(0))
                if len(parts) > 0:
                    city_part = parts[0].split('\n')
                    if len(city_part) > 0:
                        components['city'] = city_part[-1].strip()
                    
                    # Address is everything before city
                    if len(city_part) > 1:
                        components['address'] = '\n'.join(city_part[:-1]).strip()
                    
                # Try to extract zip code after state
                if len(parts) > 1:
                    zip_match = re.search(r'(\d{5}(?:-\d{4})?)', parts[1])
                    if zip_match:
                        components['zip_code'] = zip_match.group(1)
        
        return components