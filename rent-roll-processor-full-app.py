# For OpenAI API
from openai import OpenAI
import os
import json
import io
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import glob
import time
import random
from collections import Counter

import streamlit as st
import pandas as pd

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload, MediaIoBaseDownload


DRIVE_FOLDER_ID = "1KZtAaDjilfUk-trWDZX1ePvErt24iEGa"
FEEDBACK_FILENAME = "feedback_log.csv"

def main():
    st.title("Rent Roll Processing App - Step 1: Standardization Only")
    
    # Initialize all session state variables
    if "original_drive_id" not in st.session_state:
        st.session_state.original_drive_id = None
    if "standardized_drive_id" not in st.session_state:
        st.session_state.standardized_drive_id = None
    if "standardization_correct" not in st.session_state:
        st.session_state.standardization_correct = False
    if "llm_feedback_submitted" not in st.session_state:
        st.session_state.llm_feedback_submitted = False
    if "original_file_saved" not in st.session_state:
        st.session_state.original_file_saved = False
    if "standardized_file_saved" not in st.session_state:
        st.session_state.standardized_file_saved = False
    if "processed_llm_file_saved" not in st.session_state:
        st.session_state.processed_llm_file_saved = False

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

    try:
        sheet_data = pd.read_excel(uploaded_file, sheet_name=0, header=None)
        
        # Save original file to drive only if not already saved
        if not st.session_state.original_file_saved:
            uploaded_file.seek(0)
            original_file_id = upload_to_drive(
                uploaded_file, 
                f"original_{uploaded_file.name}", 
                DRIVE_FOLDER_ID
            )
            st.session_state.original_drive_id = original_file_id
            st.session_state.original_file_saved = True
            st.success(f"Original file saved to Drive with ID: {original_file_id}")
        
    except Exception as e:
        st.error(f"Failed to read Excel file: {e}")
        return

    # Step 1: Standardization
    standardized_df = standardize_data(sheet_data)
    if standardized_df is None:
        return

    # Save standardized version to drive only if not already saved
    if not st.session_state.standardized_file_saved:
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='openpyxl') as writer:
            standardized_df.to_excel(writer, index=False)
        standardized_file_id = upload_to_drive(
            output, 
            f"standardized_{uploaded_file.name}", 
            DRIVE_FOLDER_ID
        )
        st.session_state.standardized_drive_id = standardized_file_id
        st.session_state.standardized_file_saved = True
        st.success(f"Standardized file saved to Drive with ID: {standardized_file_id}")

    # Single Standardization Review
    if not st.session_state.standardization_correct:
        st.subheader("Standardization Review")
        standardization_status = st.radio("Is the standardization correct?", ["Correct", "Incorrect"], key="std_status")
        standardization_comments = st.text_area("Comments on Standardization", "", key="std_comments")

        if standardization_status == "Correct":
            button_label = "Submit Review and Continue to LLM Processing"
        else:
            button_label = "Submit Feedback"

        if st.button(button_label):
            service = get_drive_service()
            feedback_file_id = get_feedback_file_id(service)
            feedback_df = load_feedback_log(service, feedback_file_id)

            # Save standardization feedback
            standardization_entry = {
                "File Name": uploaded_file.name,
                "Origin": origin,
                "Template Type": template_type,
                "File Type": file_type,
                "Stage": "Standardization",
                "Status": standardization_status,
                "Comments": standardization_comments
            }
            feedback_df = pd.concat([feedback_df, pd.DataFrame([standardization_entry])], ignore_index=True)
            save_feedback_to_drive(service, feedback_file_id, feedback_df)
            st.success("Standardization feedback submitted.")

            if standardization_status == "Correct":
                st.session_state.standardization_correct = True

    # Only proceed to LLM processing if standardization is marked as correct
    if st.session_state.standardization_correct:
        # Step 2: LLM Processing
        st.subheader("LLM Processing Results")
        processed_df = llm_processing(standardized_df)
        processed_df = processed_df.drop(columns=['Unit'])
        processed_df['Unit No.'] = processed_df['Unit No.'].astype(str)

        # List of specified columns
        specified_order = [
            'Unit No.', 
            'Floor Plan Code', 
            'Net sf', 
            'Occupancy Status / Code', 
            'Enter "F" for Future Lease', 
            'Market Rent', 
            'Lease Start Date', 
            'Lease Expiration', 
            'Lease Term (months)', 
            'Move In Date', 
            'Move Out Date'
        ]

        # Ensure all specified columns exist in the DataFrame
        existing_columns = [col for col in specified_order if col in processed_df.columns]

        # Get remaining columns not in the specified list
        remaining_columns = [col for col in processed_df.columns if col not in existing_columns]

        # Reorder the DataFrame
        processed_df = processed_df[existing_columns + remaining_columns]

        processed_df = processed_df.sort_values(by=['Unit No.'])
        processed_df = processed_df.reset_index(drop=True)

        if processed_df is not None:
            st.success("LLM Processing completed successfully!")
            display_df_with_unique_cols(processed_df, "Final Processed Data:")

            # Save processed data to drive only if not already saved
            if not st.session_state.processed_llm_file_saved:
                output = io.BytesIO()
                with pd.ExcelWriter(output, engine='openpyxl') as writer:
                    processed_df.to_excel(writer, index=False)
                processed_file_id = upload_to_drive(
                    output, 
                    f"processed_llm_{uploaded_file.name}", 
                    DRIVE_FOLDER_ID
                )
                st.session_state.processed_llm_file_saved = True
                st.success(f"Processed file saved to Drive with ID: {processed_file_id}")

            # LLM Output Review
            if not st.session_state.llm_feedback_submitted:
                st.subheader("LLM Output Review")
                llm_status = st.radio("Is the LLM output correct?", ["Correct", "Incorrect"], key="llm_status")
                llm_comments = st.text_area("Comments on LLM Output", "", key="llm_comments")

                if st.button("Submit LLM Output Feedback"):
                    service = get_drive_service()
                    feedback_file_id = get_feedback_file_id(service)
                    feedback_df = load_feedback_log(service, feedback_file_id)

                    llm_entry = {
                        "File Name": uploaded_file.name,
                        "Origin": origin,
                        "Template Type": template_type,
                        "File Type": file_type,
                        "Stage": "LLM Processing",
                        "Status": llm_status,
                        "Comments": llm_comments
                    }
                    feedback_df = pd.concat([feedback_df, pd.DataFrame([llm_entry])], ignore_index=True)
                    save_feedback_to_drive(service, feedback_file_id, feedback_df)
                    st.session_state.llm_feedback_submitted = True
                    st.success("LLM output feedback submitted.")
            else:
                st.success("LLM feedback has already been submitted for this file.")

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

    if "Unit No." not in df.columns:
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

    unique_units = unit_df['Unit No.'].nunique()
    st.write(f"Number of unique units identified: {unique_units}")

    if unique_units == 0:
        st.warning("No unique units detected. Check the 'Unit' column or breaking point logic.")

    return unit_df

def find_breaking_point(data):
    for index, row in data.iterrows():
        if pd.notnull(row.get('Unit No.')):
            lease_start_exists = 'Lease Start Date' in data.columns
            rent_columns = [col for col in data.columns if 'rent' in col.lower()]
            if not (
                ('Net sf' not in row or (pd.notnull(row.get('Net sf')) and float(row.get('Net sf', 0)) < 10000)) and
                (any(pd.notnull(row[col]) and float(row[col]) < 10000 for col in rent_columns) or (lease_start_exists and pd.notnull(row.get('Lease Start Date'))))
            ):
                return index

            if 'Occupancy Status' in data.columns:
                if pd.notnull(row.get('Occupancy Status / Code')) and not isinstance(row.get('Occupancy Status / Code'), str):
                    return index

            if 'Charge Codes' in data.columns:
                if pd.notnull(row.get('Charge Codes')) and not isinstance(row.get('Charge Codes'), str):
                    return index
        else:
            if pd.notnull(row.get('Net sf')) or pd.notnull(row.get('Market Rent')):
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
    - Unit No.: Includes variations such as:
        - "Unit", "Unit Id", "Unit Number", "bldg-unit", "apt #", "apt number"
        - Columns containing the substring "Id" can be mapped to "Unit" only if no other "Unit"-related columns (e.g., "Unit", "Unit Number", etc.) are available.
        - Avoid "Unit No.": Clearly specifies that this rule applies only to the "Unit" column and not to "Unit No.".
    - Floor Plan Code: Includes variations like "Floor Plan", "Plan Code", "Floorplan", "Unit Type"
    - Net sf: Includes variations like "Sqft", "Unit Sqft", "Square Feet", "Sq. Ft."
    -  Occupancy Status / Code: Includes variations like "Unit Status", "Lease Status", "Occupancy", "Unit/Lease Status"
    - Market Rent: Includes variations like "Market Rent", "Market + Addl.", 'Gross Market Rent'
    - Lease Start Date: Includes variations like "Lease Start", "Lease Start Date", "Start of Lease"
    - Lease Expiration: Includes variations like "Lease End", "Lease End Date", "Lease Expiration Date"
    - Move In Date: Includes variations like "Move-In", "Move In Date", "Move In"
    - Move Out Date: Includes variations like "Move-Out", "Move Out Date", "Move Out"
    - Charge Codes: Includes variations like "Trans Code", "Charge Codes", "Description"
    - Charges or credits: these are charges in dollar amount (which is different from charge code)

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
    {"standardized_headers": ["Unit No.", "Floor Plan Code", "Net sf", "Occupancy Status / Code"]}
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

def standardize_headers_with_retries(headers_to_standardize, instructions_prompt, client, max_retries=5):
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
            cols[idx] = f"{col}_{counts[col]}"
            counts[col] += 1
        else:
            counts[col] = 0
            if duplicates[idx]:
                cols[idx] = f"{col}"

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



def llm_processing(unit_df):
    # Next, we move onto chunking and LLM processing
    st.write("Processing LLM Output...")


    def create_unit_based_batches(data, unit_column, batch_units=1, overlap_units=0):
        """
        Creates unit-based batches from the given DataFrame and converts each batch into a nested JSON format.

        Parameters:
        - data: pandas DataFrame containing the rent roll data.
        - unit_column: Column name that identifies units.
        - batch_units: Number of units per batch.
        - overlap_units: Number of units to overlap between batches.

        Returns:
        - List of JSON objects representing the nested structure for each batch.
        """
        batches = []

        # Forward-fill NaN rows to associate them with units
        data['unit_group'] = data[unit_column].fillna(method='ffill')

        # Identify unique units
        unique_units = data['unit_group'].unique()

        # Format date columns
        date_cols = ['Lease Start Date', 'Lease Expiration', 'Move In Date', 'Move Out Date']
        for col in date_cols:
            if col in data.columns:
                data[col] = pd.to_datetime(data[col], errors='coerce').dt.strftime('%Y-%m-%d')

        # Create batches
        start = 0
        while start < len(unique_units):
            end = start + batch_units
            selected_units = unique_units[start:end]

            # Filter the DataFrame for rows corresponding to the selected units
            batch = data[data['unit_group'].isin(selected_units)]
            batches.append(batch.drop(columns=['unit_group']))

            # Move to the next batch with overlap
            start += (batch_units - overlap_units)

        # Convert batches to JSON format with nested structure
        json_batches = []
        for batch in batches:
            # Convert to JSON records while preserving data as it is
            records = json.loads(batch.to_json(orient="records", date_format="iso"))

            # Filter out null values from each record
            filtered_records = []
            for record in records:
                cleaned_record = {k: v for k, v in record.items() if pd.notnull(v)}
                if cleaned_record:
                    filtered_records.append(cleaned_record)

            # Create nested structure dynamically
            nested_output = {}
            unit_key = None

            for record in filtered_records:
                # If the record has a "Unit" key, treat it as the main unit record
                if "Unit No." in record and record["Unit No."]:
                    unit_key = record["Unit No."]
                    nested_output[unit_key] = record
                    nested_output[unit_key]["Nested Values"] = []
                else:
                    # If no "Unit" key, treat the record as a nested entry for the current unit
                    if unit_key in nested_output:
                        nested_output[unit_key]["Nested Values"].append(record)

            json_batches.append(nested_output)

        return json_batches

    
    unit_batches = create_unit_based_batches(unit_df, unit_column='Unit No.')
    st.write(f'Number of unit batches: {len(unit_batches)}')

    instructions_prompt =  """
    You are an AI assistant specialized in cleaning and structuring JSON rental unit data. Your role is to read the JSON input provided by the user, which may not be fully structured, and produce a cleaned JSON output that captures all relevant data exactly as provided in the input. You are strictly prohibited from adding, inferring, or guessing any values or fields not explicitly present in the input JSON.

       Always extract and map these columns if present:
       - Unit No.
       - Floor Plan Code
       - Net sf
       - Occupancy Status / Code
       - Enter "F" for Future Lease
       - Market Rent
       - Lease Start Date
       - Lease Expiration
       - Lease Term (months)
       - Move In Date
       - Move Out Date
       - on this there might be charge codes and other value

        1. Extract Exact Fields and Values:
        Only use fields (keys) and data explicitly present in the JSON input.
        Do not create new fields or guess unknown fields or values.
        If any field in the JSON is missing or its value is empty (null, "", or similar), preserve the field but set its value to null.
        2. Handling Specific Fields:
        Floor Plan Code: If not explicitly available in the input JSON, include it in the output and set its value to null.
        Occupancy Status / Code: If not explicitly available in the input JSON, include it in the output and set its value to null.
        Other Fields: If a field is completely absent from the input JSON, omit it from the output. If the field exists but contains empty data, set its value to null.
        3. JSON Structure:
        The final output should be an array of objects, where each object corresponds to a single unit from the input JSON.
        Each key in the JSON object should match exactly a key from the input JSON.
        Convert numeric fields (e.g., Net sf, Market Rent) to numbers if possible.
        Identify date-like fields (e.g., Move In Date, Lease Expiration) convert date fields to the format "YYYY-MM-DD" if they are valid dates. If not, set the value to null.
        Do not include additional fields or rename existing fields.


        No Assumptions or Inferences:
        Do not assume or infer values for missing fields or data.
        If Floor Plan Code or Occupancy Status is missing, explicitly include them in the output with a value of null.
        For numeric fields, if the value is missing or empty, do not assume it to be zero; leave it as null.


        6. Examples:
        Input 1:
        {
          "input": {
            "Unit No.": 3041,
            "Floor Plan Code": "26301B1",
            "Net sf": 1076,
            "Resident": "t0648599",
            "Name": "Umasreemukha Siddhanthi",
            "Market Rent": 2293.1,
            "Market Rent/Sqft": 2.131134,
            "Actual Rent": 2236.0,
            "Actual Rent/Sqft": 2.078067,
            "Resident Deposit": 0.0,
            "Other Deposits": 0,
            "Move In Date": "2024-08-25",
            "Lease Start Date": "2024-08-25",
            "Lease Expiration": "2025-10-24",
            "Move Out Date": null
          }
        }

        Output 1:

        {
          "output": {
            "3041": [
              {
                "Unit No.": 3041,
                "Floor Plan Code": "26301B1",
                "Net sf": 1076,
                "Occupancy Status / Code": "Occupied",
                "Market Rent": 2293.1,
                "Lease Start Date": "2024-08-25",
                "Lease Expiration": "2025-10-24",
                "Move In Date": "2024-08-25",
                "Actual Rent": 2236.0
              }
            ]
          }
        }
        
        
        
    Input 2:
        {
          "1-1101": {
            "Unit No.": "1-1101",
            "Floor Plan Code": "W15B1",
            "Net sf": 845.0,
            "Resident": "t0533113",
            "Name": "Miguel Garcia Vasquez",
            "Market Rent": 1015.0,
            "Charge Codes": "rent",
            "Amount": 1025.0,
            "Resident Deposit": 0.0,
            "Other Deposit": 0.0,
            "Move In Date": "2019-07-15",
            "Lease Expiration": "2024-09-14",
            "Balance": 0.0,
            "Nested Values": [
              {
                "Charge Codes": "resins",
                "Amount": 10.0
              },
              {
                "Charge Codes": "ubfee",
                "Amount": 0.2
              },
              {
                "Charge Codes": "ubfee",
                "Amount": 1.99
              },
              {
                "Charge Codes": "ubfee",
                "Amount": 0.21
              },
              {
                "Charge Codes": "vtrash",
                "Amount": 25.0
              },
              {
                "Charge Codes": "pest",
                "Amount": 5.0
              },
              {
                "Charge Codes": "Total",
                "Amount": 1067.4
              }
            ]
          }
        }
        
    Output 2:
    {'1-1101': [{'Unit No.': '1-1101',
   'Floor Plan Code': 'W15B1',
   'Net sf': 845,
   'Occupancy Status / Code': 'Occupied',
   'Market Rent': 1015,
   'Lease Expiration': '2024-09-14',
   'Move In Date': '2019-07-15',
   'pest': 5,
   'rent': 1025,
   'resins': 10,
   'ubfee': 2.4,
   'vtrash': 25}]}
        """

    # Directory to save individual outputs
    output_dir = 'model_outputs_parallel'

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)


    def majority_voting(user_prompt, num_requests=5):
        """
        Implements the majority voting technique for JSON responses from an LLM.

        Args:
            request_function (callable): A function to send a request to the LLM and get a JSON response.
            input_data (str): The input data to send to the LLM.
            num_requests (int): Number of requests to send to the LLM (default is 5).

        Returns:
            dict: The JSON response with the majority vote, or a random one if no majority exists.
        """
        # Collect JSON responses
        responses = [process_unit_batches(instructions_prompt, user_prompt) for _ in range(num_requests)]
        
        # Serialize responses to make them hashable
        serialized_responses = [json.dumps(response, sort_keys=True) for response in responses]
        
        # Count occurrences of each response
        response_counts = Counter(serialized_responses)
        # Find the most common response
        most_common_serialized, frequency = response_counts.most_common(1)[0]
        
        # Deserialize the response back to JSON
        most_common_response = json.loads(most_common_serialized)
        
        # Check if there is a majority
        if frequency > 1:
            return most_common_response
        else:
            # Return a random response if there's no majority
            random_serialized = random.choice(serialized_responses)
            return json.loads(random_serialized)

    # Function to process a single batch
    def process_single_batch(idx_batch):
        idx, batch = idx_batch
        user_prompt = json.dumps(batch)
        #print(user_prompt)
        # Get the model's output
        model_output = majority_voting(user_prompt)
        # Save the raw model output to a file
        output_file = os.path.join(output_dir, f'model_output_{idx}.json')
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(model_output)
        return idx  # Return idx to identify which batch was processed

    # Function to process unit batches and save outputs in parallel
    def process_and_save_outputs_parallel(unit_batches, instructions_prompt):
        total_batches = len(unit_batches)
        start_time = time.time()

        # Use ThreadPoolExecutor to process batches in parallel
        with ThreadPoolExecutor(max_workers=50) as executor:
            futures = {executor.submit(process_single_batch, (idx, batch)): idx for idx, batch in enumerate(unit_batches)}

            # Display progress in Streamlit
            progress_bar = st.progress(0)
            for i, future in enumerate(as_completed(futures)):
                idx = futures[future]
                try:
                    result_idx = future.result()
                    # Update progress
                    progress = (i + 1) / total_batches
                    progress_bar.progress(progress)
                except Exception as e:
                    st.error(f'An error occurred while processing batch {idx}: {e}')

        elapsed_time = time.time() - start_time
        st.write(f'All batches processed in {elapsed_time:.2f} seconds.')

    client = OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

    def process_unit_batches(instructions_prompt, prompt):

        messages = [
            {"role": "system", "content": instructions_prompt},
            {"role": "user", "content": """

                        Your primary goal is to process the provided JSON input, which might not be fully structured, and convert it into a clean, structured JSON output. Follow these rules strictly:

                        Input Specifications:
                        You will receive a JSON input containing rows of unit data, possibly with missing or improperly formatted fields.
                        Your task is to clean, reformat, and structure the data as per the requirements below.

                        Processing Rules:
                        Field Inclusion:
                        Only include fields explicitly present in the input JSON.
                        Do not infer or create new fields unless explicitly mentioned in the rules.

                        Handling Missing/Empty Fields:
                        For missing fields:
                        Omit them entirely from the JSON output.
                        For fields with null, "", or invalid data:
                        Keep them as null.

                        Formatting Requirements:
                        Dates:
                        Convert any valid date fields to the format YYYY-MM-DD.
                        Numeric Fields:
                        Ensure numeric fields (e.g., Sqft, Market Rent) are represented as numbers.
                        Text Fields:
                        Maintain original text values as-is.
                        For invalid values, leave them as null.
                        Special Fields:
                        If the fields Floor Plan Code or Occupancy Status are missing, include them with a value of null.

                        Dynamic/Charge code Fields:
                        Retain any additional fields (dynamic columns) present in the input JSON without modification.

                        Output Requirements:
                        Structure the output as an array of objects, where each object represents a single unit’s data.
                        Make sure that output has data only from input JSON
                        Maintain the original field names from the input JSON
                        Ensure data types are consistent and align with the input values:
                        Numbers for numeric fields
                        Strings for text fields
                        Null for missing or empty values
                        Do not introduce assumptions or external data

                        Here’s the JSON input:

                     """ + prompt},
                     ]
        #print(prompt)
        #st.write(prompt)
        response = client.chat.completions.create(
            model="ft:gpt-4o-mini-2024-07-18:radix:rent-roll-processor-v01:Age52GN8",
            messages=messages,
          response_format={
            "type": "json_object"
          },
            temperature=0,
            max_completion_tokens=1000,
            top_p=0
        )

        return response.choices[0].message.content
    # Process unit batches in parallel
    with st.spinner('Processing unit batches in parallel...'):
        process_and_save_outputs_parallel(unit_batches, instructions_prompt)

    # Combine saved outputs
    def combine_saved_outputs(output_dir='model_outputs_parallel'):
        # Initialize a list to hold parsed outputs
        parsed_outputs = []

        # Get all output files
        output_files = sorted(glob.glob(os.path.join(output_dir, 'model_output_*.json')))

        for output_file in output_files:
            with open(output_file, 'r', encoding='utf-8') as f:
                model_output = f.read()
                # Parse the model's output as JSON
                try:
                    output_json = json.loads(model_output)
                    parsed_outputs.append(output_json)
                except json.JSONDecodeError as e:
                    st.error(f"Error decoding JSON from {output_file}: {e}")

        # Initialize an empty dictionary to hold the combined data
        combined_data = {}

        for output in parsed_outputs:
            for unit, records in output.items():
                if unit not in combined_data:
                    # Initialize as a list if records is a list, otherwise create a new list
                    combined_data[unit] = [records] if isinstance(records, dict) else records
                else:
                    existing_records = combined_data[unit]
                    # Ensure existing_records is a list
                    if not isinstance(existing_records, list):
                        existing_records = [existing_records]
                        combined_data[unit] = existing_records

                    # Handle incoming records
                    if isinstance(records, list):
                        # Convert existing records to strings for comparison
                        existing_record_strings = [json.dumps(r, sort_keys=True) for r in existing_records]
                        for record in records:
                            record_string = json.dumps(record, sort_keys=True)
                            if record_string not in existing_record_strings:
                                existing_records.append(record)
                                existing_record_strings.append(record_string)
                    else:
                        # Handle single record case
                        record_string = json.dumps(records, sort_keys=True)
                        existing_record_strings = [json.dumps(r, sort_keys=True) for r in existing_records]
                        if record_string not in existing_record_strings:
                            existing_records.append(records)

        return combined_data

    combined_data = combine_saved_outputs()
    # Convert combined data to DataFrame
    rows = []
    def flatten_data(unit, details):
        if isinstance(details, list):
            for item in details:
                rows.append({
                    'Unit': unit, **item})
        elif isinstance(details, dict):
            rows.append({'Unit': unit, **details})
        else:
            rows.append({'Unit': unit, 'Details': details})

    for unit, details in combined_data.items():
        flatten_data(unit, details)

    if rows:
        llm_df = pd.DataFrame(rows)
        display_df_with_unique_cols(llm_df.head(), "LLM Output Data:")
        return llm_df
    else:
        st.error("No data was extracted by the LLM.")
        return None

def save_feedback(file_name, origin, template_type, file_type, status, comments, stage):
    feedback = {
        "File Name": file_name,
        "Origin": origin,
        "Template Type": template_type,
        "File Type": file_type,
        "Stage": stage,
        "Status": status,
        "Comments": comments
    }

    # Append feedback to a CSV file
    feedback_file = "feedback_log.csv"
    feedback_df = pd.DataFrame([feedback])

    if os.path.exists(feedback_file):
        feedback_df.to_csv(feedback_file, mode='a', header=False, index=False)
    else:
        feedback_df.to_csv(feedback_file, index=False)
        
if __name__ == "__main__":
    main()
