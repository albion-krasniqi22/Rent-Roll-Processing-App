import streamlit as st
import pandas as pd
import os
import json
import io
import warnings
warnings.filterwarnings("ignore")

# For OpenAI API
import openai
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


DRIVE_FOLDER_ID = "1A5TaBdAnA9JQZ73H6ckFgPEytQ_nSPMD"
FEEDBACK_FILENAME = "feedback_log.csv"

def main():
    st.title("Rent Roll Processing App - Step 1: Standardization Only")
    
    if "original_drive_id" not in st.session_state:
        st.session_state.original_drive_id = None
    if "standardized_drive_id" not in st.session_state:
        st.session_state.standardized_drive_id = None

    # Sidebar for file metadata selection
    st.sidebar.header("File Metadata")
    origin = st.sidebar.selectbox("Origin", ["Successful RedIQ Processing", "Failed RedIQ Processing"])
    template_type = st.sidebar.selectbox("Template Type", ["OneSite", "Yardi", "Resman", "Entrada", "AMSI", "Other"])
    file_type = st.sidebar.selectbox("File Type", ["Single-line Data Rows", "Multi-line Data Rows"])

    uploaded_file = st.file_uploader("Upload Rent Roll Excel File (.xlsx only)", type=["xlsx", "xls"])

    if uploaded_file:
        st.write("**Debugging Mode:** Focus on standardization only.")
        st.write("Steps:")
        st.write("1. Reading file.")
        st.write("2. Identifying header rows.")
        st.write("3. Standardizing headers using LLM.")
        st.write("4. Ensuring essential columns found.")
        st.write("5. Applying breaking point logic.")
        st.write("6. Confirming unit count.")

        process_file(uploaded_file, origin, template_type, file_type)

def get_drive_service():
    # Construct credentials from TOML secrets instead of a JSON file
    credentials_info = {
        "type": "service_account",
        "project_id": st.secrets["project_id"],
        "private_key_id": st.secrets["private_key_id"],
        "private_key": st.secrets["private_key"],
        "client_email": st.secrets["client_email"],
        "client_id": st.secrets["client_id"],
        "auth_uri": st.secrets["auth_uri"],
        "token_uri": st.secrets["token_uri"],
        "auth_provider_x509_cert_url": st.secrets["auth_provider_x509_cert_url"],
        "client_x509_cert_url": st.secrets["client_x509_cert_url"]
    }

    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    service = build('drive', 'v3', credentials=credentials)
    return service

def get_feedback_file_id(service):
    query = f"parents = '{DRIVE_FOLDER_ID}' and name = '{FEEDBACK_FILENAME}' and mimeType='text/csv'"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if files:
        return files[0]['id']
    else:
        # Create an empty feedback_log.csv in Drive
        empty_data = "File Name,Origin,Template Type,File Type,Stage,Status,Comments\n"
        media = MediaIoBaseUpload(io.BytesIO(empty_data.encode('utf-8')), mimetype='text/csv')
        file_metadata = {
            'name': FEEDBACK_FILENAME,
            'parents': [DRIVE_FOLDER_ID],
            'mimeType': 'text/csv'
        }
        file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
        return file.get('id')

def load_feedback_log(service, file_id):
    request = service.files().get_media(fileId=file_id)
    fh = io.BytesIO()
    downloader = MediaIoBaseDownload(fh, request)
    done = False
    while not done:
        status, done = downloader.next_chunk()
    fh.seek(0)
    content = fh.read().decode('utf-8', errors='replace')
    if content.strip() == "":
        return pd.DataFrame(columns=["File Name","Origin","Template Type","File Type","Stage","Status","Comments"])
    else:
        return pd.read_csv(io.StringIO(content))

def save_feedback_to_drive(service, file_id, df):
    csv_data = df.to_csv(index=False)
    media = MediaIoBaseUpload(io.BytesIO(csv_data.encode('utf-8')), mimetype='text/csv', resumable=True)
    service.files().update(fileId=file_id, media_body=media).execute()

def upload_to_drive(file_content, filename, folder_id, mime_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'):
    # file_content is a BytesIO or file-like object
    file_content.seek(0)
    service = get_drive_service()
    file_metadata = {
        'name': filename,
        'parents': [folder_id]
    }
    media = MediaIoBaseUpload(file_content, mimetype=mime_type, resumable=True)
    uploaded_file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
    return uploaded_file.get('id')

def process_file(uploaded_file, origin, template_type, file_type):
    st.write("Processing file:", uploaded_file.name)

    # Load the original file from the uploaded buffer directly into pandas
    # No local saving
    try:
        sheet_data = pd.read_excel(uploaded_file, sheet_name=0, header=None)
    except Exception as e:
        st.error(f"Failed to read Excel file: {e}")
        return

    display_df_with_unique_cols(sheet_data.head(), "Original Data:")

    standardized_df = standardize_data(sheet_data)
    if standardized_df is None:
        return

    # Convert original file (uploaded_file) to a BytesIO for upload
    uploaded_file.seek(0)
    original_file_content = io.BytesIO(uploaded_file.read())
    original_file_content.seek(0)

    # Convert standardized_df to a BytesIO (Excel in memory)
    standardized_buffer = io.BytesIO()
    standardized_df.to_excel(standardized_buffer, index=False, engine='openpyxl')
    standardized_buffer.seek(0)

    # Upload both original and standardized files to Google Drive only if not done before
    if st.session_state.original_drive_id is None and st.session_state.standardized_drive_id is None:
        original_drive_id = upload_to_drive(original_file_content, uploaded_file.name, DRIVE_FOLDER_ID)
        standardized_drive_id = upload_to_drive(standardized_buffer, f'standardized_{uploaded_file.name}', DRIVE_FOLDER_ID)
        st.success(f"Original file uploaded to Google Drive. File ID: {original_drive_id}")
        st.success(f"Standardized data uploaded to Google Drive. File ID: {standardized_drive_id}")

        st.session_state.original_drive_id = original_drive_id
        st.session_state.standardized_drive_id = standardized_drive_id
    else:
        st.write("Files already uploaded to Google Drive:")
        st.write(f"Original Drive ID: {st.session_state.original_drive_id}")
        st.write(f"Standardized Drive ID: {st.session_state.standardized_drive_id}")

    # Standardization Review
    st.subheader("Standardization Review")
    standardization_status = st.radio("Is the standardization correct?", ["Correct", "Incorrect"], key="std_status")
    standardization_comments = st.text_area("Comments on Standardization", "", key="std_comments")

    if st.button("Submit Standardization Feedback"):
        service = get_drive_service()
        feedback_file_id = get_feedback_file_id(service)
        feedback_df = load_feedback_log(service, feedback_file_id)

        new_entry = {
            "File Name": uploaded_file.name,
            "Origin": origin,
            "Template Type": template_type,
            "File Type": file_type,
            "Stage": "Standardization",
            "Status": standardization_status,
            "Comments": standardization_comments
        }
        # Use pd.concat since append is deprecated
        feedback_df = pd.concat([feedback_df, pd.DataFrame([new_entry])], ignore_index=True)

        save_feedback_to_drive(service, feedback_file_id, feedback_df)
        st.success("Standardization feedback submitted.")
        st.success("Feedback log updated on Google Drive.")

def standardize_data(sheet_data):
    keywords = [
        'unit', 'unit id', 'unit number', 'unit no', 'unit designation',
        'move-in', 'move in', 'movein', 'move-in date', 'move in date', 'moveindate',
        'move-out', 'move out', 'moveout', 'move-out date', 'move out date', 'moveoutdate',
        'lease', 'lease start', 'lease start date', 'lease begin', 'start of lease',
        'lease end', 'lease end date', 'lease expiration', 'end of lease',
        'rent', 'market rent', 'lease rent', 'market + addl.', 'market',
        'unit status', 'lease status', 'occupancy', 'unit/lease status',
        'floorplan', 'floor plan',
        'sqft', 'sq ft', 'square feet', 'square ft', 'square footage', 'sq. ft.', 'sq.ft',
        'unit sqft', 'unit size',
        'code', 'charge code', 'trans code', 'transaction code', 'description'
    ]

    if sheet_data.empty:
        st.error("The provided sheet is empty. Cannot proceed.")
        return None

    normalized_data = sheet_data.applymap(lambda x: str(x).lower() if pd.notnull(x) else '')
    normalized_data['keyword_count'] = normalized_data.apply(
        lambda row: sum(row.str.contains('|'.join(keywords), regex=True)),
        axis=1
    )

    header_candidates = normalized_data[normalized_data['keyword_count'] >= 3]
    if header_candidates.empty:
        st.error("No suitable header rows found. Header identification failed.")
        return None
    else:
        display_df_with_unique_cols(header_candidates.head(), "Header Candidates:")

    selected_header_df = merge_and_select_first_header_to_bottom(header_candidates, 'keyword_count', keywords)
    if selected_header_df.empty:
        st.error("No suitable merged header row found. Check the input file.")
        return None
    else:
        display_df_with_unique_cols(selected_header_df, "Selected Header Row:")

    sheet_data.columns = selected_header_df.iloc[0, :-1]
    data_start_idx = selected_header_df.index[0] + 1
    df = sheet_data[data_start_idx:].reset_index(drop=True)
    df.columns = df.columns.str.strip()

    if len(df.columns) == 0:
        st.error("No columns found after setting headers. Aborted.")
        return None

    st.write("**Debug Info:** Columns before GPT standardization:", list(df.columns))

    instructions_prompt = standardization_instructions()
    headers_to_standardize = list(df.columns)
    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])
    standardized_headers = standardize_headers_with_retries(headers_to_standardize, instructions_prompt, client)

    if not standardized_headers:
        st.error("GPT-based standardization failed after multiple attempts.")
        return None

    standardized_headers = make_column_names_unique(standardized_headers)
    df.columns = standardized_headers

    st.write("**Debug Info:** Columns after GPT standardization:", list(df.columns))

    if "Unit" not in df.columns:
        st.error("No 'Unit' column found. Possibly failed to map a unit column.")
        return None

    if df.empty:
        st.error("No data rows remain after initial cleaning.")
        return None

    df = df.dropna(how='all')
    df = df.replace({r'[\*,]': ''}, regex=True)

    before_filtering_count = len(df)
    df = df[df.apply(lambda row: any(pd.to_numeric(row, errors='coerce').notnull()), axis=1)]
    st.write(f"Dropped {before_filtering_count - len(df)} rows with no numeric values.")
    df.reset_index(drop=True, inplace=True)

    st.write("Applying breaking point logic...")
    st.write(f"DataFrame shape before breaking point: {df.shape}")
    breaking_point = find_breaking_point(df)

    if breaking_point is not None:
        st.write(f"Breaking point found at row {breaking_point}.")
        unit_df = df[:breaking_point]
    else:
        st.write("No breaking point found. Using entire DataFrame as unit data.")
        unit_df = df

    unit_df.dropna(axis=0, how='all', inplace=True)
    unit_df.dropna(axis=1, how='all', inplace=True)
    st.write("DataFrame shape after breaking point filtering:", unit_df.shape)

    display_df_with_unique_cols(unit_df, "Final Standardized Data (All Rows):")

    unique_units = unit_df['Unit'].nunique()
    st.write(f"Number of unique units identified: {unique_units}")

    if unique_units == 0:
        st.warning("No unique units detected. Check the 'Unit' column or breaking point logic.")

    return unit_df

def find_breaking_point(data):
    for index, row in data.iterrows():
        if pd.notnull(row.get('Unit')):
            lease_start_exists = 'Lease Start Date' in data.columns
            if not (
                (pd.notnull(row.get('Sqft')) and float(row.get('Sqft', 0)) < 10000) and
                (pd.notnull(row.get('Market Rent')) or (lease_start_exists and pd.notnull(row.get('Lease Start Date'))))
            ):
                return index

            if 'Occupancy Status' in data.columns:
                if pd.notnull(row.get('Occupancy Status')) and not isinstance(row.get('Occupancy Status'), str):
                    return index

            if 'Charge Codes' in data.columns:
                if pd.notnull(row.get('Charge Codes')) and not isinstance(row.get('Charge Codes'), str):
                    return index
        else:
            if pd.notnull(row.get('Sqft')) or pd.notnull(row.get('Market Rent')):
                return index
            if 'Charge Codes' in data.columns:
                if pd.notnull(row.get('Charge Codes')) and row.isnull().all():
                    return index

    return None

def merge_and_select_first_header_to_bottom(df, keyword_column, keywords):
    df = df.sort_index()
    merged_header = None
    final_header = None

    for idx, row in df.iterrows():
        if merged_header is None:
            merged_header = row
            final_header = row
            continue

        if idx - merged_header.name == 1:
            combined_row = merged_header[:-1] + " " + row[:-1]
            combined_keyword_count = sum(combined_row.str.contains('|'.join(keywords), regex=True))
            if combined_keyword_count > merged_header[keyword_column]:
                row[:-1] = combined_row
                row[keyword_column] = combined_keyword_count
                final_header = row
            continue

        break

    if final_header is not None:
        return pd.DataFrame([final_header])
    else:
        return pd.DataFrame([])

def standardization_instructions():
    instructions_prompt = """
    We aim to standardize headers across multiple documents to ensure consistency and ease of processing. Below are examples of how various column names might appear in different documents and the standardized format we want to achieve:

    Standardized Column Headers:
    - Unit: Includes variations like "Unit", "Unit Id", "Unit Number", "Unit No.", "bldg-unit"
    - Floor Plan Code: Includes variations like "Floor Plan", "Plan Code", "Floorplan"
    - Sqft: Includes variations like "Sqft", "Unit Sqft", "Square Feet", "Sq. Ft."
    - Occupancy Status: Includes variations like "Unit Status", "Lease Status", "Occupancy", "Unit/Lease Status"
    - Market Rent: Includes variations like "Market Rent", "Market + Addl.", 'Gross Market Rent'
    - Lease Start Date: Includes variations like "Lease Start", "Lease Start Date", "Start of Lease"
    - Lease Expiration: Includes variations like "Lease End", "Lease End Date", "Lease Expiration Date"
    - Move In Date: Includes variations like "Move-In", "Move In Date", "Move In"
    - Move-Out Date: Includes variations like "Move-Out", "Move Out Date", "Move Out"
    - Charge Codes: Includes variations like "Trans Code", "Charge Codes", "Description"
    - Charges or credits: this is Charges as in dollar amount (which is differeent from charge code)

    Examples of Standardized Headers:
    Unit No., Floor Plan Code, Sqft, Occupancy Status, Market Rent, Lease Start Date, Lease Expiration, Move In Date, Move-Out Date, Charge Codes

    Task:
    Your task is to analyze the headers provided in a list and map each header to its corresponding standardized column name. If a header does not match any standardized category, retain it as-is.

    Key Details:
    1. The input is a list of column names.
    2. The output must be a list of the same size, with each header mapped to its standardized name or retained as-is if no match is found.
    3. Be mindful of slight differences in naming, abbreviations, or spacing in headers. Use the examples above as a reference for mapping.
    4. If a header is unclear or does not match a category, make an educated guess or retain the original formatting with corrections for consistency.
    5. If a specific rule or example is not provided, update the header format to follow Pascal Case and ensure clarity. Apply your best judgment to map headers to the standardized list or format them consistently while preserving their original intent.

    Task:
    1. Standardize the provided headers according to the categories above.
    2. Return the result as a JSON object with a key 'standardized_headers' containing the list of standardized headers.
    3. Preserve empty strings as they are.
    4. Apply consistent formatting (Pascal Case, clarity, etc.)
    5. If no clear standardization exists, keep the original header.

    Example Input:
    ['unit', 'floorplan', 'sqft', 'unit/lease status']

    Example Output:
    {"standardized_headers": ["Unit", "Floor Plan Code", "Sqft", "Occupancy Status"]}
    """
    return instructions_prompt

def gpt_model(instructions_prompt, header, client):
    headers_str = ", ".join(repr(h) for h in header)
    messages = [
        {"role": "system", "content": instructions_prompt},
        {"role": "user", "content": f"Standardize these headers: {headers_str}"}
    ]

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_format={"type": "json_object"}
    )

    response_content = response.choices[0].message.content
    try:
        standardized_headers = json.loads(response_content)['standardized_headers']
    except (json.JSONDecodeError, KeyError):
        try:
            standardized_headers = eval(response_content)
        except:
            standardized_headers = None

    return standardized_headers

def standardize_headers_with_retries(headers_to_standardize, instructions_prompt, client, max_retries=4):
    attempt = 0
    standardized_headers = None

    while attempt < max_retries and standardized_headers is None:
        attempt += 1
        try:
            with st.spinner(f'GPT Standardization Attempt {attempt}/{max_retries}...'):
                standardized_headers = gpt_model(instructions_prompt, headers_to_standardize, client)
        except Exception as e:
            st.warning(f"GPT attempt {attempt} failed: {e}")
            standardized_headers = None

        if standardized_headers is not None and len(standardized_headers) != len(headers_to_standardize):
            st.warning("GPT returned mismatched number of headers.")
            standardized_headers = None

    return standardized_headers

def make_column_names_unique(column_names):
    cols = pd.Series(column_names).fillna('Unnamed').replace('', 'Unnamed')
    duplicates = cols.duplicated(keep=False)
    counts = {}
    for idx, col in enumerate(cols):
        if col in counts:
            counts[col] += 1
            cols[idx] = f"{col}_{counts[col]}"
        else:
            counts[col] = 0
            if duplicates[idx]:
                cols[idx] = f"{col}_{counts[col]}"

    return cols.tolist()

def display_df_with_unique_cols(df, message=""):
    if message:
        st.write(message)

    display_df = df.copy()
    seen = {}
    new_cols = []
    for col in display_df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}" if col else f"Unnamed_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col if col else "Unnamed")

    display_df.columns = new_cols
    st.dataframe(display_df)

if __name__ == "__main__":
    main()
