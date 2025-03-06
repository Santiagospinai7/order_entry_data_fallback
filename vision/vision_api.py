import datetime
import io
import os
import re

import pdfplumber
from google.cloud import vision
from pdf2image import convert_from_path

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r'googlecreds.json'
poppler_path = r'poppler-24.02.0\Library\bin'

def organize_annotations(text_annotations):

    # Step 2: Create a dictionary to map each text annotation to its (x, y) coordinates
    text_dict = {}

    # Step 3: Populate the dictionary with the (x, y) coordinates
    for i, annotation in enumerate(text_annotations):
        vertices = annotation.bounding_poly.vertices
        x_values = [vertex.x for vertex in vertices]
        y_values = [vertex.y for vertex in vertices]
        min_x = min(x_values)
        min_y = min(y_values)
        text_dict[i] = (min_x, min_y)

    # Step 4: Sort the dictionary by the y-coordinate values (in ascending order) and then by the x-coordinate values (in ascending order)
    sorted_text = [text_annotations[i] for i in
                   sorted(text_dict.keys(), key=lambda i: (text_dict[i][1], text_dict[i][0]))]

    # Step 5: Group the text annotations by horizontal position
    horizontal_groups = []
    try:
        current_group = [sorted_text[0]]
    except:
        print('Index out of range.')
        print(text_annotations)
    previous_x = text_dict[0][0]
    for annotation in sorted_text[1:]:
        x = text_dict[sorted_text.index(annotation)][0]
        if abs(x - previous_x) <= 10:  # Set a threshold for the maximum distance between annotations in the same group
            current_group.append(annotation)
        else:
            horizontal_groups.append(current_group)
            current_group = [annotation]
        previous_x = x
    horizontal_groups.append(current_group)

    # Step 6: Create a list of lists, where each sub-list contains the text annotations that are in the same horizontal position
    organized_text = []
    for group in horizontal_groups:
        annotations = [annotation.description for annotation in group]
        organized_text.append(annotations)

    organized_text.pop(0)
    concatenated_text = ''
    for sublist in organized_text:
        concatenated_text += ' '.join(sublist) + ' '
    # Step 2: Remove leading/trailing whitespace
    concatenated_text = concatenated_text.strip()
    return concatenated_text

# Create a function to extract all text from pdf using Google Vision API
def extract_data_from_pdf(hire_info):
    """Detects document features in an image."""
    client = vision.ImageAnnotatorClient()
    for file in hire_info['downloads']:
        if file.endswith('.pdf'):
            images = convert_from_path(file, poppler_path=poppler_path)
            pdf_text = ''
            for i, image in enumerate(images):
                img_dir = file[:-4] + 'page' + str(i) + '.jpg'
                image.save(img_dir, 'JPEG')

                with io.open(img_dir, 'rb') as image_file:
                    content = image_file.read()

                image_file = vision.Image(content=content)

                response = client.document_text_detection(image=image_file)
                document = response.full_text_annotation
                pdf_text += document.text
                if response.text_annotations:
                    concat_text = organize_annotations(response.text_annotations)

        if 'HireRightMVR' in file:
            MVRdate = re.search('Date Request Submitted: (\w{3} \d\d?, \d{4}) \d\d?:\d\d? \wM PDT\\nRequest Completion Date: (\w{3} \d\d?, \d{4}) \d\d?:\d\d?', pdf_text)
            if MVRdate:
                if 'MVR' not in hire_info.keys() and MVRdate:
                    hire_info['MVR'] = (datetime.datetime.strptime(MVRdate.group(1), '%b %d, %Y')).strftime('%m-%d-%Y')
                elif MVRdate:
                    mvr_date = (datetime.datetime.strptime(MVRdate.group(2), '%b %d, %Y'))
                    if mvr_date > datetime.datetime.strptime(hire_info['MVR'], '%m-%d-%Y'):
                        hire_info['MVR'] = mvr_date.strftime('%m-%d-%Y')
            else:
                print('could not pick up mvr data for candidate')

        if any(x in file.lower() for x in ['Med Card','Medcard']):
            physical_due = re.search("Medical Examiner\'s Certificate Expiration .*?\n(\d\d?(?:-|\/)\d\d?(?:-|\/)\d{2,4})", pdf_text)
            if physical_due:
                hire_info['physical_due'] = physical_due.group(1)
            else:
                print('could not pick up med card data for candidate')

        if 'Generic Inbound' in file:
            full_text = ''
            with pdfplumber.open(file) as pdf:
                for page in pdf.pages:
                    text = page.extract_text()
                    table = page.extract_table()
                    full_text += text
            table.pop(0)
            full_text = full_text.replace('\n', '')
            try:
                hire_info['Address'] = re.search('STREET ADDRESS : (.*?)STATE', concat_text).group(1)
                hire_info['phone'] = re.search('HOME # : (.*?) ', concat_text).group(1) if re.search('HOME # : (.*?) ', concat_text) else re.search('HOME # : (.*?) ', full_text)
                hire_info['email'] = re.search('EMAIL ADDRESS : (.*?) CELL', concat_text).group(1)
                hire_info['dob'] = re.search('BIRTH .*?(\d\d-\d\d-\d{4})', concat_text).group(1)
                hire_info['contacts'] = [[item[0],item[3], item[6]] for item in table]
            except:
                print('could not parse the generic inbound pdf.')
                print(full_text)

        if 'Per Diem Pay' in file:
            hire_info['Perdiem'] = True if re.search('XI would like to participate', pdf_text) else False


    return hire_info


