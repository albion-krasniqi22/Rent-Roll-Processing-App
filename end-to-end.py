# For OpenAI API
import openai
from openai import OpenAI
from dotenv import load_dotenv
import os
import json
import io
import warnings
from concurrent.futures import ThreadPoolExecutor, as_completed
import shutil
import glob
import time

load_dotenv()

import streamlit as st
import pandas as pd
import os
import json

# For OpenAI API
import openai
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

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



def llm_processing(unit_df):
    # Next, we move onto chunking and LLM processing
    st.write("Processing LLM Output...")

    def create_unit_based_batches(data, unit_column, batch_units=1, overlap_units=0):
        """
        Split a DataFrame into overlapping batches based on units.

        Args:
            data (pd.DataFrame): Input DataFrame to split into batches.
            unit_column (str): Column name identifying units.
            batch_units (int): Maximum number of units in each batch. Default is 1.
            overlap_units (int): Number of overlapping units between batches. Default is 0.

        Returns:
            list of pd.DataFrame: List of overlapping DataFrame batches.
        """
        batches = []
        data['unit_group'] = data[unit_column].fillna(method='ffill')  # Forward-fill NaN rows to associate them with units
        unique_units = data['unit_group'].unique()  # Identify unique units

        start = 0
        while start < len(unique_units):
            # Determine the range of unique units for the current batch
            end = start + batch_units
            selected_units = unique_units[start:end]

            # Filter the DataFrame for rows corresponding to the selected units
            batch = data[data['unit_group'].isin(selected_units)]
            batches.append(batch.drop(columns=['unit_group']))  # Drop the helper column before returning

            # Move to the next batch with overlap
            start += (batch_units - overlap_units)

        return batches

    unit_batches = create_unit_based_batches(unit_df, unit_column='Unit')
    st.write(f'Number of unit batches: {len(unit_batches)}')

    instructions_prompt =  """
    You are an AI assistant specialized in converting CSV rental unit data into a structured JSON format. Your role is to read the CSV input provided by the user and produce a JSON output that accurately captures all relevant unit information.

    **Instructions:**

    1. **Required Core Fields:**
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
       - Leave Column Blank (if present, handle gracefully by ignoring or leaving it out if empty)

       If any of these fields are missing from the CSV, do your best to infer or leave them out if no logical inference can be made.

    2. **Dynamic Fields (Charge Codes and Other Columns):**
    The CSV may include additional columns beyond the core fields (e.g., "Actual Rent", "Trash", "Garage", "Misc"). Ensure that any additional columns present in the input CSV are included as key-value pairs under each unit's JSON object, preserving their corresponding values. Do not discard any columns containing relevant data.
    
    Important: Only include fields and values that exist in the input CSV. Do not generate or infer any values that are not explicitly present in the input.


    3. **Formatting Details:**
       - The final output should be a JSON object or an array of objects, where each object represents a single unit’s data.
       - Convert all date fields into the format "YYYY-MM-DD".
       - Represent numeric values (like Net sf, Market Rent) as numbers where possible.
       - Include all relevant fields from the CSV. If a value is missing or empty, you may omit that field or set it to null.

    4. **Data Integrity:**
       - If columns are missing, make reasonable assumptions or leave them out.
       - Map Occupancy Status / Code based on available data. If the unit appears occupied or has a tenant, consider it "Occupied".
       - Derive the Floor Plan Code or Lease Term if such information can be inferred from the CSV or leave it out if not available.

    5. **Example:**
       Given input similar to:

        Unit,Sqft,Tenant Name,Market Rent,Misc,Move In Date,Lease Expiration,Move Out Date
        201,1065,Regina Hawkins,975,3,09/01/2023,08/31/2024,
    
        Produce:
        {201: [{'Unit No.': 201, 'Floor Plan Code': 'E', 'Net sf': 1065, 'Occupancy Status / Code': 'Occupied', 'Market Rent': 975, 'Lease Expiration': '2024-08-31', 'Move In Date': '2023-09-01', 'Enter individual charge codes into the blue cells below (ex. rent, conc, petf, etc). For each unit, enter charge amounts in the corresponding columns Actual Rent': 975, 'Misc': 0}]}

        """
    # Set your OpenAI API key securely (already set in standardize_data)
    # openai.api_key = st.secrets["OPENAI_API_KEY"]

    # Directory to save individual outputs
    output_dir = 'model_outputs_parallel'

    if os.path.exists(output_dir):
        shutil.rmtree(output_dir)
    os.makedirs(output_dir)

    # Function to process a single batch
    def process_single_batch(idx_batch):
        idx, batch = idx_batch
        # Convert the input DataFrame to CSV format (string)
        user_prompt = batch.to_csv(index=False)
        # Get the model's output
        model_output = process_unit_batches(instructions_prompt, user_prompt)
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
        with ThreadPoolExecutor(max_workers=30) as executor:
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
            {"role": "user", "content": "Your primary goal: Analyze the user-provided CSV input and convert it into a JSON structure that only captures the information present in the CSV file. Do not assume or infer new values, columns, or fields. Only include data that is explicitly present in the CSV file, including dynamic columns. If a value is missing or empty, set it to null or omit it entirely. Your output must strictly reflect the contents of the CSV file without introducing any external data or assumptions.\n\n" +prompt},
        ]

        response = client.chat.completions.create(
            model="ft:gpt-4o-mini-2024-07-18:radix:rent-roll-processor:AfEoZsW7",
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
                    combined_data[unit] = records
                else:
                    existing_records = combined_data[unit]
                    if isinstance(records, list):
                        for record in records:
                            if record not in existing_records:
                                existing_records.append(record)
                    else:
                        if records not in existing_records:
                            existing_records.append(records)

        return combined_data

    combined_data = combine_saved_outputs()
    # Convert combined data to DataFrame
    rows = []
    def flatten_data(unit, details):
        if isinstance(details, list):
            for item in details:
                rows.append({'Unit': unit, **item})
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
