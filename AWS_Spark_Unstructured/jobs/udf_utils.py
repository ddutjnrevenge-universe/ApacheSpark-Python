import re
from datetime import datetime

def extract_file_names(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    return position

def extract_position(file_content):
    file_content = file_content.strip()
    position = file_content.split('\n')[0]
    # Only uppercase the first letter of each word
    position = ' '.join([word.capitalize() for word in position.split()])
    return position

def extract_classcode(file_content):
    try:
        classcode_match = re.search(r'Class Code:\s+(\d+)', file_content)
        classcode = classcode_match.group(1) if classcode_match else None
        return classcode
    except Exception as e:
        raise ValueError(f'Error extracting class code: {e}')

def extract_start_date(file_content):
    try:
        # Correct the regex pattern: remove extra closing parenthesis
        opendate_match = re.search(r'Open [Dd]ate:\s+(\d\d-\d\d-\d\d)', file_content)
        
        # Use group(1) since there is only one capturing group
        start_date = datetime.strptime(opendate_match.group(1), '%m-%d-%y') if opendate_match else None
        # print(start_date)
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
    try:
        req_match = re.search(r'(REQUIREMENTS?/\s?MINIMUM QUALIFICATIONS?)(.*)(PROCESS NOTES?)', file_content, 
                              re.DOTALL)
        req = req_match.group(2).strip() if req_match else None
        return req
    except Exception as e:
        raise ValueError(f'Error extracting requirements: {str(e)}')

def extract_notes(file_content):
    try:
        notes_match = re.search(r'(NOTES?):(.*)(?=DUTIES)', file_content, 
                              re.DOTALL | re.IGNORECASE)
        notes = notes_match.group(2).strip() if notes_match else None
        return notes
    except Exception as e:
        raise ValueError(f'Error extracting notes: {str(e)}')

def extract_duties(file_content):
    try:
        duties_match = re.search(r'(DUTIES?):(.*)(REQ[A-Z])', file_content, 
                              re.DOTALL)
        duties = duties_match.group(2).strip() if duties_match else None
        return duties
    except Exception as e:
        raise ValueError(f'Error extracting duties: {str(e)}')

def extract_selection(file_content):
    try:
        selection_match = re.findall(r'([A-Z][a-z]+)(\s\.\s)+', file_content)
        selection = [match[0] for match in selection_match] if selection_match else None
        return selection
    except Exception as e:
        raise ValueError(f'Error extracting selection: {str(e)}')
    
def extract_experience_length(file_content):
    try:
        experience_length_match = re.search(
            r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)\s(years?)\s(of\sfull(-|\s)time)',
            file_content)
        experience_length = experience_length_match.group(1) if experience_length_match else None
        return experience_length
    except Exception as e:
        raise ValueError(f'Error extracting experience length: {str(e)}')

def extract_education_length(file_content):
    try:
        education_length_match = re.search(
            r'(One|Two|Three|Four|Five|Six|Seven|Eight|Nine|Ten|one|two|three|four|five|six|seven|eight|nine|ten)(\s|-)(years?)\s(college|university)',
            file_content)
        education_length = education_length_match.group(1) if education_length_match else None
        return education_length
    except Exception as e:
        raise ValueError(f'Error extracting education length: {str(e)}')

def extract_application_location(file_content):
    try:
        application_location_match = re.search(r'(Applications? will only be accepted on-?line)', file_content, re.IGNORECASE)
        application_location = 'Online' if application_location_match else 'Mail or In Person'
        return application_location
    except Exception as e:
        raise ValueError(f'Error extracting application location: {str(e)}')