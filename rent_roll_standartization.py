import streamlit as st
import pandas as pd
import os
import json
import tempfile
import warnings
warnings.filterwarnings("ignore")

# For OpenAI API
import openai
from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

def main():
    st.title("Rent Roll Processing App - Step 1: Standardization Only")

    # Sidebar for file metadata selection
    st.sidebar.header("File Metadata")
    origin = st.sidebar.selectbox("Origin", ["Successful RedIQ Processing", "Failed RedIQ Processing"])
    template_type = st.sidebar.selectbox("Template Type", ["OneSite", "Yardi", "Resman", "Entrada", "AMSI", "Other"])
    file_type = st.sidebar.selectbox("File Type", ["Single-line Data Rows", "Multi-line Data Rows"])

    # File upload
    uploaded_file = st.file_uploader("Upload Rent Roll Excel File (.xlsx only)", type=["xlsx", "xls"])

    if uploaded_file:
        st.write("**Debugging Mode:** You are focusing only on standardization.")
        st.write("**Steps:**")
        st.write("1. Reading file.")
        st.write("2. Identifying header rows.")
        st.write("3. Attempting to standardize headers using an LLM model.")
        st.write("4. Ensuring essential columns (like 'Unit') are found.")
        st.write("5. Applying breaking point logic to isolate unit rows.")
        st.write("6. Confirming unit count.")


        # Process the file
        process_file(uploaded_file, origin, template_type, file_type)

def process_file(uploaded_file, origin, template_type, file_type):
    st.write("Processing file:", uploaded_file.name)
    # Save uploaded file to a temporary location
    with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
        fp = tmp_file.name
        tmp_file.write(uploaded_file.getbuffer())

    try:
        # Read the Excel file
        sheet_data = pd.read_excel(fp, sheet_name=0, header=None)
    except Exception as e:
        st.error(f"Failed to read Excel file: {e}")
        return

    st.write("**Raw Data (first few rows):**")
    display_df_with_unique_cols(sheet_data.head(), "Original Data:")

    # Step 1: Standardization
    standardized_df = standardize_data(sheet_data)
    if standardized_df is None:
        st.error("Standardization could not be completed. Check the logs for details.")
        return

    # Save standardized data to Excel
    base_name, _ = os.path.splitext(uploaded_file.name)
    standardized_output_path = os.path.join('outputs', f'standardized_{base_name}.xlsx')
    os.makedirs('outputs', exist_ok=True)
    
    # Use openpyxl engine for xlsx format
    standardized_df.to_excel(standardized_output_path, index=False, engine='openpyxl')
    st.success(f"Standardized data saved to {standardized_output_path}")

    # Standardization Review
    st.subheader("Standardization Review")
    standardization_status = st.radio("Is the standardization correct?", ["Correct", "Incorrect"], key="std_status")
    standardization_comments = st.text_area("Comments on Standardization", "", key="std_comments")

    # Save feedback
    if st.button("Submit Standardization Feedback"):
        save_feedback(uploaded_file.name, origin, template_type, file_type,
                      standardization_status, standardization_comments, "Standardization")
        st.success("Standardization feedback submitted.")

def standardize_data(sheet_data):
    """
    Standardizes the data by:
    1. Identifying and merging header rows.
    2. Using the GPT model (with retries) to standardize headers.
    3. Verifying that essential columns are present.
    4. Cleaning data further.
    5. Applying breaking point logic and displaying the number of unique units.
    """

    # Define the keywords for identifying header rows
    keywords = [
        # Unit-related
        'unit', 'unit id', 'unit number', 'unit no', 'unit designation',
        # Move-in/out dates
        'move-in', 'move in', 'movein', 'move-in date', 'move in date', 'moveindate',
        'move-out', 'move out', 'moveout', 'move-out date', 'move out date', 'moveoutdate',
        # Lease-related
        'lease', 'lease start', 'lease start date', 'lease begin', 'start of lease',
        'lease end', 'lease end date', 'lease expiration', 'end of lease',
        # Rent-related
        'rent', 'market rent', 'lease rent', 'market + addl.', 'market',
        # Occupancy status
        'unit status', 'lease status', 'occupancy', 'unit/lease status',
        # Floor plan
        'floorplan', 'floor plan',
        # Square footage
        'sqft', 'sq ft', 'square feet', 'square ft', 'square footage', 'sq. ft.', 'sq.ft',
        'unit sqft', 'unit size',
        # Codes and transactions
        'code', 'charge code', 'trans code', 'transaction code', 'description'
    ]

    if sheet_data.empty:
        st.error("The provided sheet is empty. Cannot proceed with standardization.")
        return None

    # Normalize data for header identification
    normalized_data = sheet_data.applymap(lambda x: str(x).lower() if pd.notnull(x) else '')
    normalized_data['keyword_count'] = normalized_data.apply(
        lambda row: sum(row.str.contains('|'.join(keywords), regex=True)),
        axis=1
    )

    # Potential header candidates
    header_candidates = normalized_data[normalized_data['keyword_count'] >= 3]

    if header_candidates.empty:
        st.error("No suitable header rows found. Header identification failed.")
        return None
    else:
        st.write("**Debug Info:** Candidate header rows identified:")
        display_df_with_unique_cols(header_candidates.head(), "Header Candidates:")

    # Select and merge the first valid header row
    selected_header_df = merge_and_select_first_header_to_bottom(header_candidates, 'keyword_count', keywords)

    if selected_header_df.empty:
        st.error("No suitable merged header row found. Please check the input file.")
        return None
    else:
        st.write("**Debug Info:** Selected and merged header row:")
        display_df_with_unique_cols(selected_header_df, "Selected Header Row:")

    # Set headers and trim top rows
    sheet_data.columns = selected_header_df.iloc[0, :-1]
    data_start_idx = selected_header_df.index[0] + 1
    df = sheet_data[data_start_idx:].reset_index(drop=True)
    df.columns = df.columns.str.strip()

    if len(df.columns) == 0:
        st.error("No columns found after setting headers. Standardization aborted.")
        return None

    st.write("**Debug Info:** Columns detected before GPT standardization:")
    st.write(list(df.columns))

    # GPT Standardization step
    instructions_prompt = standardization_instructions()
    headers_to_standardize = list(df.columns)

    client = OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
    standardized_headers = standardize_headers_with_retries(headers_to_standardize, instructions_prompt, client)

    if not standardized_headers:
        st.error("GPT-based standardization failed after multiple attempts.")
        return None

    standardized_headers = make_column_names_unique(standardized_headers)
    df.columns = standardized_headers

    st.write("**Debug Info:** Columns after GPT standardization:")
    st.write(list(df.columns))

    # Ensure 'Unit' column is present
    if "Unit" not in df.columns:
        st.error("No 'Unit' column found. Possibly the standardization failed to map a unit column.")
        return None

    # Ensure the DataFrame is not empty
    if df.empty:
        st.error("No data rows remain after initial cleaning.")
        return None

    # Further cleaning steps
    # Drop rows with all NaNs and replace special characters
    df = df.dropna(how='all')
    df = df.replace({r'[\*,]': ''}, regex=True)

    # Drop rows containing only strings (no numeric or date-like values)
    before_filtering_count = len(df)
    df = df[df.apply(lambda row: any(pd.to_numeric(row, errors='coerce').notnull()), axis=1)]
    st.write(f"**Debug Info:** Dropped {before_filtering_count - len(df)} rows that had no numeric values.")
    df.reset_index(drop=True, inplace=True)

    # Apply breaking point logic
    st.write("**Debug Info:** Applying breaking point logic...")
    st.write(f"DataFrame shape before breaking point logic: {df.shape}")
    breaking_point = find_breaking_point(df)

    if breaking_point is not None:
        st.write(f"**Debug Info:** Breaking point found at row index {breaking_point}.")
        unit_df = df[:breaking_point]
    else:
        st.write("**Debug Info:** No breaking point found. Using entire DataFrame as unit data.")
        unit_df = df

    # Clean final unit_df
    unit_df.dropna(axis=0, how='all', inplace=True)
    unit_df.dropna(axis=1, how='all', inplace=True)
    st.write("**Debug Info:** DataFrame shape after breaking point filtering:", unit_df.shape)

    # Display final standardized data
    display_df_with_unique_cols(unit_df, "Final Standardized Data (All Rows):")

    # Show number of unique units to confirm correct unit identification
    unique_units = unit_df['Unit'].nunique()
    st.write(f"**Debug Info:** Number of unique units identified: {unique_units}")

    if unique_units == 0:
        st.warning("No unique units detected. Check if the 'Unit' column or breaking point logic is correct.")

    return unit_df

def find_breaking_point(data):
    """
    Identify the "breaking point" in the DataFrame where rows no longer represent valid unit data.
    This logic helps separate actual unit data from any subsequent summary rows or extra data.
    """
    for index, row in data.iterrows():
        if pd.notnull(row.get('Unit')):
            # Validate numeric fields
            lease_start_exists = 'Lease Start Date' in data.columns
            if not (
                (pd.notnull(row.get('Sqft')) and float(row.get('Sqft', 0)) < 10000) and
                (pd.notnull(row.get('Market Rent')) or
                 (lease_start_exists and pd.notnull(row.get('Lease Start Date'))))
            ):
                return index

            # Ensure Occupancy Status is a string if present
            if 'Occupancy Status' in data.columns:
                if pd.notnull(row.get('Occupancy Status')) and not isinstance(row.get('Occupancy Status'), str):
                    return index

            # Ensure Charge Codes is a string if present
            if 'Charge Codes' in data.columns:
                if pd.notnull(row.get('Charge Codes')) and not isinstance(row.get('Charge Codes'), str):
                    return index
        else:
            # If Unit is absent, ensure no unit-specific numeric fields present
            if pd.notnull(row.get('Sqft')) or pd.notnull(row.get('Market Rent')):
                return index

            if 'Charge Codes' in data.columns:
                if pd.notnull(row.get('Charge Codes')) and row.isnull().all():
                    return index

    return None

def merge_and_select_first_header_to_bottom(df, keyword_column, keywords):
    # Merge header rows if needed
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
            combined_keyword_count = sum(
                combined_row.str.contains('|'.join(keywords), regex=True)
            )

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

    # Parse the response
    try:
        standardized_headers = json.loads(response_content)['standardized_headers']
    except (json.JSONDecodeError, KeyError):
        try:
            standardized_headers = eval(response_content)
        except:
            standardized_headers = None

    return standardized_headers

def standardize_headers_with_retries(headers_to_standardize, instructions_prompt, client, max_retries=2):
    attempt = 0
    standardized_headers = None

    while attempt < max_retries and standardized_headers is None:
        attempt += 1
        try:
            with st.spinner(f'GPT Standardization Attempt {attempt}/{max_retries}...'):
                standardized_headers = gpt_model(instructions_prompt, headers_to_standardize, client)
        except Exception as e:
            st.warning(f"GPT-based standardization attempt {attempt} failed due to an error: {e}")
            standardized_headers = None

        if standardized_headers is not None and len(standardized_headers) != len(headers_to_standardize):
            st.warning("GPT-based standardization returned a mismatched number of headers.")
            standardized_headers = None

    return standardized_headers

def make_column_names_unique(column_names):
    cols = pd.Series(column_names)
    cols = cols.fillna('Unnamed')
    cols = cols.replace('', 'Unnamed')

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

    feedback_file = "feedback_log.csv"
    feedback_df = pd.DataFrame([feedback])

    if os.path.exists(feedback_file):
        feedback_df.to_csv(feedback_file, mode='a', header=False, index=False)
    else:
        feedback_df.to_csv(feedback_file, index=False)

def display_df_with_unique_cols(df, message=""):
    if message:
        st.write(message)

    display_df = df.copy()
    seen = {}
    new_cols = []
    for col in display_df.columns:
        if col in seen:
            seen[col] += 1
            new_cols.append(f"{col}_{seen[col]}" if col != '' else f"Unnamed_{seen[col]}")
        else:
            seen[col] = 0
            new_cols.append(col if col != '' else "Unnamed")

    display_df.columns = new_cols
    st.dataframe(display_df)

if __name__ == "__main__":
    main()