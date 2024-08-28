import re
from datetime import datetime

def extract_file_names(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_position(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_classcode(file_content):
    try:
        classcode_match = re.search(r'Class Code:)\s+(\d+)', file_content)
        classcode = classcode_match.group(2) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f'Error extracting class node: {e}')

def extract_start_date(file_content):
    try:
        # Correct the regex pattern: remove extra closing parenthesis
        opendate_match = re.search(r'Open [Dd]ate:\s+(\d\d-\d\d-\d\d)', file_content)
        
        # Use group(1) since there is only one capturing group
        start_date = datetime.strptime(opendate_match.group(1), '%m-%d-%y') if opendate_match else None
        print(start_date)
        return start_date
    except Exception as e:
        raise ValueError(f'Error extracting start date: {e}')
    
# def extract_end_date(file_content):
#     enddate_match = re.search(
#         r'JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER\s(\d{1,2},\s\d{4})',
#         file_content)
#     endate = enddate_match.group() if enddate_match else None
#     endate = datetime.strptime(endate, '%B %d, %Y') if endate else None  
#     return endate
def extract_end_date(file_content):
    # Update the regex pattern to capture both month, day, and year
    enddate_match = re.search(
        r'(JANUARY|FEBRUARY|MARCH|APRIL|MAY|JUNE|JULY|AUGUST|SEPTEMBER|OCTOBER|NOVEMBER|DECEMBER)\s(\d{1,2}),\s(\d{4})',
        file_content
    )
    
    # Extract the matched date string correctly
    if enddate_match:
        endate = enddate_match.group()  # Use .group() to get the full match
        try:
            # Convert the extracted date string to a datetime object
            endate = datetime.strptime(endate, '%B %d, %Y')
        except ValueError as e:
            raise ValueError(f"Error parsing end date: {e}")
    else:
        endate = None

    return endate

def extract_salary(file_content):
    try: 
        salary_pattern = r'\$(\d{1,3}(?:,\d{3})+).+?to.+\$(\d{1,3}(?:,\d{3})+)(?:\s+and\s+\$(\d{1,3}(?:,\d{3})+)\s+to\s+\$(\d{1,3}(?:,\d{3})+))?'
        salary_match = re.search(salary_pattern, file_content)
        
        if salary_match:
            salary_start = float(salary_match.group(1).replace(',', ''))
            salary_end = float(salary_match.group(4).replace(',', '')) if salary_match.group(4) \
                else float(salary_match.group(2).replace(',', ''))
        else:
            salary_start = None
            salary_end = None
        return salary_start, salary_end
    except Exception as e:
        raise ValueError(f'Error extracting salary: {str(e)}')




def extract_req(file_content):
    pass

def extract_notes(file_content):
    pass

def extract_duties(file_content):
    pass

def extract_selection(file_content):
    pass

def extract_experience_length(file_content):
    pass

def extract_education_length(file_content):
    pass

def extract_application_location(file_content):
    pass