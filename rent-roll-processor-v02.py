import streamlit as st
import os
import json
import io
import pandas as pd
import openai
from openai import OpenAI
import re
import concurrent.futures
import numpy as np
import glob

import warnings
warnings.filterwarnings("ignore")

# =============== 1. Put all your function definitions here ===============

client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

# For demonstration, I'm including only the key ones. You can copy all from your script.
def display_headers_info(df, message=""):
    """
    Print a message and then show a few rows to see the headers visually (in Streamlit).
    """
    if message:
        st.write(message)
    st.dataframe(df.head())
    #st.write("Columns:", list(df.columns))
    #st.write("Shape:", df.shape)
    st.write("-" * 100)
    st.write("")

def load_excel_file(filepath):
    """
    Load the Excel file into a pandas DataFrame with no header (header=None).
    """
    try:
        df = pd.read_excel(filepath, sheet_name=0, header=None)
        return df
    except Exception as e:
        st.error(f"Error reading Excel file: {e}")
        return None

def read_top_rows(file_buffer, max_rows=10):
    """
    Reads the top 'max_rows' from the uploaded Excel file (no header)
    and converts them into a single text block.
    """
    df_top = pd.read_excel(file_buffer, nrows=max_rows, header=None)
    lines = []
    for _, row in df_top.iterrows():
        row_str = " | ".join(str(x) for x in row if pd.notnull(x))
        lines.append(row_str.strip())
    top_text = "\n".join(lines)
    return top_text


def extract_property_info_via_gpt(text_block, model="gpt-3.5-turbo"):    
    # Example system or user instructions
    system_instructions = """
    You are an assistant specialized in reading the top text of a rent roll file.
    Your task is to find the 'property name' and the 'as_of_date' from the text provided.
    If the date is missing or invalid, set it to null. Return the result as JSON.
    Schema:
    {
      "property_name": string or null,
      "as_of_date": string in "YYYY-MM-DD" format or null
    }
    """
    
    user_prompt = f"""
    Text from the top of an Excel file:
    --------------
    {text_block}
    --------------
    Please parse the property name and the as-of date.
    Return as JSON: {{ "property_name": "...", "as_of_date": "YYYY-MM-DD" }}
    If you can't find them, set them to null.
    """
    
    response = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": system_instructions},
            {"role": "user", "content": user_prompt}
        ],
        temperature=0
    )
    
    # Extract the content
    content = response.choices[0].message.content.strip()
    
    # Attempt to parse JSON
    try:
        data = json.loads(content)
        prop_name = data.get("property_name")
        as_of_date = data.get("as_of_date")
        return prop_name, as_of_date
    except Exception as e:
        st.error(f"Error calling GPT API or parsing JSON: {e}")
        return None, None


def get_property_info(filepath):
    """
    1. Read the top text lines from the Excel file.
    2. Ask GPT for property name & as_of_date.
    3. Return them (or None if not found).
    """
    top_text = read_top_rows(filepath, max_rows=10)
    # Now call GPT to parse
    property_name, as_of_date = extract_property_info_via_gpt(top_text)

    #print("Property Name:", property_name)
    #print("As of date:", as_of_date)
    
    return property_name, as_of_date

def identify_header_candidates(sheet_data, keywords):
    """
    Find rows that contain multiple keyword matches as candidate header rows.
    Returns a DataFrame of candidate header rows (if any).
    """
    # Convert all cells to lowercase strings (handle NaNs)
    normalized_data = sheet_data.applymap(lambda x: str(x).lower() if pd.notnull(x) else '')
    
    # Count how many keywords appear in each row
    normalized_data['keyword_count'] = normalized_data.apply(
        lambda row: sum(row.str.contains('|'.join(keywords), regex=True)), axis=1
    )
    
    # Candidate rows: containing >=3 hits from the keyword list
    header_candidates = normalized_data[normalized_data['keyword_count'] >= 3]
    
    return header_candidates


def merge_and_select_first_header_to_bottom(df, keyword_column, keywords):
    """
    If consecutive rows might each contain partial headers, 
    this attempts to merge them to create a single best header row.
    Returns a single-row DataFrame representing the merged header.
    """
    df = df.sort_index()
    merged_header = None
    final_header = None

    for idx, row in df.iterrows():
        if merged_header is None:
            merged_header = row
            final_header = row
            continue

        # If the row is right after the merged_header, try merging
        if idx - merged_header.name == 1:
            combined_row = merged_header[:-1] + " " + row[:-1]
            combined_keyword_count = sum(combined_row.str.contains('|'.join(keywords), regex=True))
            
            # If the newly combined row has more keyword hits than the old one, 
            # update the final_header
            if combined_keyword_count > merged_header[keyword_column]:
                row[:-1] = combined_row
                row[keyword_column] = combined_keyword_count
                final_header = row
            continue

        # If not consecutive, break out
        break

    if final_header is not None:
        return pd.DataFrame([final_header])
    else:
        return pd.DataFrame([])


def standardization_instructions():
    """
    Returns the standardization prompt to be sent to GPT.
    """
    instructions_prompt = """
    We aim to standardize headers across multiple documents to ensure consistency and ease of processing. Below are examples of how various column names might appear in different documents and the standardized format we want to achieve:

    Standardized Column Headers:
    - Unit No.: Includes variations such as:
        - "Unit", "Unit Id", "Unit Number", "bldg-unit", "apt #", "apt number"
        - Columns containing the substring "Id" can be mapped to "Unit" only if no other "Unit"-related columns (e.g., "Unit", "Unit Number", etc.) are available.
        - Avoid "Unit No.": Clearly specifies that this rule applies only to the "Unit" column and not to "Unit No.".
    - Floor Plan Code: Includes variations like "Floor Plan", "Plan Code", "Floorplan", "Unit Type", Bd/Ba, "Type"
    - Net sf: Includes variations like "Sqft", "Unit Sqft", "Square Feet", "Sq. Ft."
    -  Occupancy Status / Code: Includes variations like "Unit Status", "Lease Status", "Occupancy", "Unit/Lease Status"
    - Market Rent: Includes variations like "Market Rent", "Market + Addl.", 'Gross Market Rent'
    - Lease Start Date: Includes variations like "Lease Start", "Lease Start Date", "Start of Lease" (not lease name)
    - Lease Expiration: Includes variations like "Lease End", "Lease End Date", "Lease Expiration Date"
    - Move In Date: Includes variations like "Move-In", "Move In Date", "Move In"
    - Move Out Date: Includes variations like "Move-Out", "Move Out Date", "Move Out"
    - Charge Codes: Includes variations like "Trans Code", "Charge Codes", "Description"
    - Amount: these are charges in dollar amount (which is different from charge code), Charges or credits

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
            standardized_headers = gpt_model(instructions_prompt, headers_to_standardize, client)
        except Exception as e:
            standardized_headers = None

        if standardized_headers is not None and len(standardized_headers) != len(headers_to_standardize):
            standardized_headers = None

    return standardized_headers



def make_column_names_unique(column_names):
    """
    If multiple columns end up with the same standardized name, 
    rename them uniquely (e.g., "Unit No.", "Unit No._1", "Unit No._2", etc.).
    """
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
                # If the very first time we see a duplicate, rename it
                cols[idx] = f"{col}_{counts[col]}"
    return cols.tolist()


def drop_unnecessary_rows(df):
    """
    Drop rows that contain no numeric values at all.
    """
    before_count = len(df)
    df = df.dropna(how='all')  # Drop any completely empty rows
    df = df.reset_index(drop=True)

    # Filter rows that have at least one numeric value
    numeric_filter = df.apply(lambda row: any(pd.to_numeric(row, errors='coerce').notnull()), axis=1)
    df = df[numeric_filter].reset_index(drop=True)

    after_count = len(df)
    print(f"Dropped {before_count - after_count} rows that contained no numeric values.")
    return df


def find_breaking_point(data):
    for index, row in data.iterrows():
        if pd.notnull(row.get('Unit No.')):
            lease_start_exists = 'Lease Start Date' in data.columns
            rent_columns = [col for col in data.columns if 'rent' in col.lower()]
            
            net_sf = row.get('Net sf')
            if pd.notnull(net_sf):
                try:
                    net_sf = float(str(net_sf).replace(',', ''))
                except ValueError:
                    net_sf = 0  # Default to 0 if conversion fails

            if not (
                ('Net sf' not in row or (pd.notnull(net_sf) and net_sf < 10000)) and
                (any(
                    pd.notnull(row[col]) and float(str(row[col]).replace(',', '')) < 10000
                    for col in rent_columns
                ) or (lease_start_exists and pd.notnull(row.get('Lease Start Date'))))
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


def finalize_columns(df):
    # Columns we expect/want to ensure exist in the final DataFrame
    desired_columns = [
        "Unit No.",
        "Floor Plan Code",
        "Net sf",
        "Occupancy Status / Code",
        'Enter "F" for Future Lease',
        "Market Rent",
        "Lease Start Date",
        "Lease Expiration",
        "Lease Term (months)",
        "Move In Date",
        "Move Out Date",
    ]

    # 0) Filter out rows that lack a valid "Unit No."
    #    i.e., exclude NaN or empty string
    if "Unit No." not in df.columns:
        print("No 'Unit No.' column found. Cannot proceed with grouping or pivoting.")
        return df

    # 1) Detect if we have the columns needed to pivot charges
    has_charge_codes = ("Charge Codes" in df.columns and "Amount" in df.columns)
    if has_charge_codes:
        print("Detected scenario with 'Charge Codes' & 'Amount'. We will pivot them.")
    else:
        print("No 'Charge Codes'/'Amount' in DataFrame => single-line scenario. No pivoting will occur.")

    # 2) Define columns to combine into the unique key
    group_cols = [c for c in df.columns if c not in ("Charge Codes", "Amount")]

    # ------------------------------------------------------------------------
    if "Amount" in df.columns:
        df["Amount"] = pd.to_numeric(df["Amount"], errors="coerce")

    # Instead of unconditional forward fill, do a "block-based" approach:
    # Mark each row that starts a new block whenever *any* group_col is non-null
    df["_new_block"] = df[group_cols].notna().any(axis=1).cumsum()

    # Forward-fill within each block, so we don't just fill forever
    df[group_cols] = df.groupby("_new_block")[group_cols].ffill()

    # Clean up helper columns
    df.drop(columns=["_new_block"], inplace=True, errors="ignore")
    # ------------------------------------------------------------------------

    # 3) Create a 'unique_key' for grouping
    df["unique_key"] = (
        df[group_cols]
        .astype(str)
        .agg('|'.join, axis=1)
        .fillna("EMPTY")
    )    

    # 4) Group by 'unique_key' using an aggregator of 'first' for each non-pivot column
    all_cols = df.columns.tolist()
    aggregations = {col: "first" for col in all_cols if col not in ["Charge Codes", "Amount"]}

    grouped = df.groupby("unique_key", as_index=False).agg(aggregations)

    # 6) Pivot 'Charge Codes' & 'Amount' if needed
    if has_charge_codes:
        pivoted_charges = (
            df.pivot_table(
                index="unique_key",
                columns="Charge Codes",
                values="Amount",
                aggfunc="sum"
            )
            .reset_index()
        )
        # Merge pivoted table with the grouped DataFrame
        grouped = pd.merge(grouped, pivoted_charges, on="unique_key", how="left")

        # Now drop 'unique_key' again if it re-appeared
        if "unique_key" in grouped.columns:
            grouped.drop(columns=["unique_key"], inplace=True)

    # 7) Ensure 'desired_columns' exist in the result
    for col in desired_columns:
        if col not in grouped.columns:
            grouped[col] = None

    # 8) Reorder columns:
    #    "Unit No." first, then the rest of desired_columns, then leftover pivoted columns
    remaining = [
        c for c in grouped.columns
        if c not in desired_columns and c != "Unit No."
    ]
    final_columns_order = (
        ["Unit No."] +
        [col for col in desired_columns if col != "Unit No."] +
        remaining
    )
    # Keep only columns that actually exist
    final_columns_order = [col for col in final_columns_order if col in grouped.columns]

    final_df = grouped[final_columns_order]
    
    if "unique_key" in final_df.columns:
        final_df.drop(columns=["unique_key"], inplace=True)

    # 9) Convert date columns to YYYY-MM-DD if present
    date_cols = [
        "Lease Start Date",
        "Lease Expiration",
        "Move In Date",
        "Move Out Date",
    ]
    for col in date_cols:
        if col in final_df.columns:
            final_df[col] = (
                pd.to_datetime(final_df[col], errors="coerce")
                .dt.strftime("%Y-%m-%d")
            )
    final_df = final_df.dropna(subset=["Unit No."]).reset_index(drop=True)
    return final_df


def process_row(row):
    """Process a single row and determine occupancy status."""
    # Prepare the row data as a string for the API
    row_data = row.to_dict()
    row_text = "\n".join([f"{key}: {value}" for key, value in row_data.items()])
    
    # Prompt for API
    prompt = f"""
    You are a data processing assistant. I will provide you with rows of data from a CSV file, each representing information about a rental unit. Your task is to categorize the Occupancy Status / Code for each row based on these rules:
        1.Occupied: The row contains a tenant name, and at least one of the following fields: lease start date, lease expiration date, or move-in date. Additional charges may also be present along with the market rent.
        2.Vacant: The tenant name is absent or displays ‘Vacant’. Lease-related fields (dates) are empty. Market rent is present, but no other charges exist.
        3.Model: Similar to Vacant, but the tenant name displays ‘Model’. Market rent is present, but no other charges or lease related information.
        4.Applicant: A tenant name is present and lease-related fields (e.g., dates) may be present. Only market rent is listed; no other charges are included.
    Based on the following data, determine the `Occupancy Status / Code` using the rules provided earlier:
    {row_text}
    Only return the category as the output (Occupied, Vacant, Model, or Applicant).
    
    Note current, notice are considered as Occupied
    """
    
    # Call the OpenAI API
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        # Extract the response
        status = response.choices[0].message.content.strip()
        return status
    except Exception as e:
        print(f"Error processing row: {e}")
        return "Error"


def label_occupancy_status_parallel(df, max_workers):
    """
    Label occupancy status for rows in a DataFrame using parallel processing.
    """
    # Apply parallel processing
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        results = list(executor.map(process_row, [row for _, row in df.iterrows()]))
    
    # Add results to the DataFrame
    df['Occupancy Status / Code'] = results
    return df

def identify_pairs(df):
    """
    Identify pairs of rows with the same unit ID and set their Occupancy Status to None.
    """
    # Group by unit ID and get groups with two or more rows
    grouped = df.groupby('Unit No.')
    eligible_unit_ids = grouped.filter(lambda x: len(x) >= 2)['Unit No.'].unique()

    # Set Occupancy Status to None for rows in eligible groups
    df.loc[df['Unit No.'].isin(eligible_unit_ids), "Occupancy Status / Code"] = None

    # Extract pairs as subsets
    pairs = [group for _, group in grouped if len(group) >= 2]

    return pairs


def process_pair(pair, property_name, as_of_date):
    """
    Process a pair of rows and determine which is Vacant and which is Applicant.
    """
    
    pair_text = "\n\n".join(
        f"Row {i + 1}:\n" + "\n".join([f"{key}: {value}" for key, value in row.items()])
        for i, row in enumerate(pair.to_dict(orient="records"))
    )
    
    #print(pair_text)
    
    prompt = f"""

        You are a data processing assistant tasked with determining the "Occupancy Status / Code" for rows representing the same unit in a property data file.

        Key Information:
        Property Name: {property_name}
        Reference Date: {as_of_date}

        Occupancy Status Definitions:
        You cannot have same status for same unit no make your best guess out of the two pairs.
        Assign one status per row using the definitions and inference rules. 


        Vacant: No occupant name or explicitly listed as "Vacant."
        Applicant: Occupant name present, and dates (move-in/lease start) are future or near the present date to {as_of_date}.
        Occupied: Occupant name present, and:
        Date is < {as_of_date}, with rent charges indicating an active/historical lease.
        Special Cases:
        Units may have multiple rows reflecting transitions (e.g., Occupied → Vacant → Applicant).
        Inference Rule: Rows for the same unit should not have the same status. Use transitions to guide decisions (e.g., if one is Occupied, the other is likely Applicant or Vacant).

        For each row, analyze: Occupant name, relevant dates (move-in/out, lease start), rent charges, and {as_of_date}.
        
        Format:
        Respond with one status per row:
        Row 1: <status>
        Row 2: <status>

        Data rows:
        {pair_text}

        Please respond with one status per row, in the format:
        Row 1: <status>
        Row 2: <status>
        
        """
    
    try:
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[{"role": "user", "content": prompt}]
        )
        # Parse the response
        result = response.choices[0].message.content.strip()
        statuses = {
            line.split(":")[0].strip(): line.split(":")[1].strip()
            for line in result.split("\n") if ":" in line
        }
        return statuses
    except Exception as e:
        print(f"Error processing pair: {e}")
        return {"Row 1": "Error", "Row 2": "Error"}

    

def process_single_pair(pair, property_name, as_of_date, max_retries=3):
    retries = 0
    success = False
    result_dict = {} 

    while retries < max_retries and not success:
        statuses = process_pair(pair, property_name, as_of_date)
        if all(statuses.get(f"Row {i + 1}", None) for i in range(len(pair))):
            for i, (index, row) in enumerate(pair.iterrows()):
                row_name = f"Row {i + 1}"
                if row_name in statuses:
                    result_dict[index] = statuses[row_name]
            success = True
        else:
            retries += 1
            #print(f"Retry {retries} for pair:\n{pair}")

    if not success:
        print(f"Failed to update statuses for pair after {max_retries} retries:\n{pair}")
        for i, (index, row) in enumerate(pair.iterrows()):
            result_dict[index] = "Error"

    return result_dict



def refine_pairs_parallel(df, pairs, property_name, as_of_date, max_retries=3, max_workers=10):
    all_results = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_to_pair = {}
        for pair in pairs:
            future = executor.submit(
                process_single_pair,
                pair,
                property_name,
                as_of_date,
                max_retries
            )
            future_to_pair[future] = pair

        for future in concurrent.futures.as_completed(future_to_pair):
            try:
                pair_result = future.result()  # dict {index: status}
                all_results.append(pair_result)
            except Exception as e:
                print(f"Error in processing pair: {e}")

    for result_dict in all_results:
        for row_index, status in result_dict.items():
            df.at[row_index, "Occupancy Status / Code"] = status

    return df


def refine_occupancy_status(df, max_workers, property_name, as_of_date, max_retries=3):
    # 1. First pass: initial categorization
    df = label_occupancy_status_parallel(df, max_workers)

    # 2. Identify pairs
    pairs = identify_pairs(df)

    # 3. Refine the pairs (parallel)
    df = refine_pairs_parallel(df, pairs, property_name, as_of_date, max_retries, max_workers)
    
    # 4. Update future lease indicator for Applicants
    df.loc[df['Occupancy Status / Code'] == 'Applicant', 'Enter "F" for Future Lease'] = 'F'

    return df


def get_columns_to_drop(columns):
    """
    Use an LLM to determine which columns should be dropped from the DataFrame.
    """
    # Generate the prompt
    column_list = ", ".join(repr(col) for col in columns)
    instructions_prompt = """
    You are a data processing assistant. I will provide you with a list of column names from a dataset 
    representing rental unit information. Your task is to determine which columns should be dropped 
    based on the following high-level rules:

    **Rules for Dropping Columns**:
    1. Do not keep any tenant-specific information:
        - Examples: Tenant name, Tenant ID, or any other identifying information.
    2. Do not keep totals, balances, or outstanding amounts:
        - Examples: Totals, total charges, balance owed, deposits etc.
    3. Do not keep calculated metrics per square footage that are not directly needed:
    4. Be conservative:
        - Keep more columns rather than accidentally dropping something essential.
        - If in doubt, retain the column. comrent keep, future lease (F)

    **Columns to Keep**:
    - Columns related to property information:
        - Examples: Market rent, rent, lease start date, lease expiration date, move-in date, unit details (e.g., net square footage), misc, trash, pet
    - Columns providing essential context for the dataset.

    Return the response as a JSON object with the following format:
    {{
        "dropped_columns": ["Column 1", "Column 2", "Column 3"]
    }}
    """
    
    # Call the GPT model
    messages = [
        {"role": "system", "content": instructions_prompt},
        {"role": "user", "content": f"Columns: {column_list}"}
    ]

    response = client.chat.completions.create(
        model="gpt-4o",
        messages=messages,
        response_format={ "type": "json_object" }

    )

    response_content = response.choices[0].message.content
    try:
        dropped_columns = json.loads(response_content)['dropped_columns']
    except (json.JSONDecodeError, KeyError):
        print("Raw response from LLM:", response_content)
        try:
            dropped_columns = eval(response_content)
        except:
            dropped_columns = None

    return dropped_columns


def drop_unnecessary_columns(df, columns_to_drop):
    """
    Drop the specified columns from the DataFrame.
    """
    df = df.drop(columns=columns_to_drop, errors='ignore')
    return df


def refine_dataframe(df):
    """
    Refine the DataFrame by identifying and dropping unnecessary columns using an LLM.
    """
    columns = df.columns.tolist()
    columns_to_drop = get_columns_to_drop(columns)
    
    # Ensure columns_to_drop is a valid list
    if not columns_to_drop:
        columns_to_drop = []

    # Now columns_to_drop is an empty list if it was None
    st.write("Columns to drop:", columns_to_drop)
    
    refined_df = drop_unnecessary_columns(df, columns_to_drop)
    return refined_df


def standardize_data_workflow(file_buffer):
    """
    End-to-end function for data standardization with Streamlit interface
    """
    # Step 1: Load Excel file
    sheet_data = load_excel_file(file_buffer)
    if sheet_data is None or sheet_data.empty:
        st.warning("No data loaded; exiting workflow.")
        return None
    
    display_headers_info(sheet_data, "Original Data:")
    
    property_name, as_of_date = get_property_info(file_buffer)
    st.write(f"**Property Name:** {property_name}")
    st.write(f"**As of Date:** {as_of_date}")

    # Define keywords
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

    # Step 2: Identify header candidates
    with st.spinner('Identifying header candidates...'):
        header_candidates = identify_header_candidates(sheet_data, keywords)
        display_headers_info(header_candidates, 'Header Candidates:')

        if header_candidates.empty:
            st.warning("No suitable header rows found; cannot proceed.")
            return None
    
    # Step 3: Merge and select the best header row
    with st.spinner('Selecting best header row...'):
        selected_header_df = merge_and_select_first_header_to_bottom(header_candidates, 'keyword_count', keywords)

        if selected_header_df.empty:
            st.warning("No suitable merged header row found; check input file.")
            return None
        
        new_header = selected_header_df.iloc[0, :-1]
        header_row_index = selected_header_df.index[0]
    
    # Step 4: Set header and clean data
    with st.spinner('Setting headers and cleaning data...'):
        sheet_data.columns = new_header.values
        data_start_idx = header_row_index + 1
        df = sheet_data[data_start_idx:].reset_index(drop=True)
        #display_headers_info(df, "Data After Setting Headers (Raw):")
        
        # Clean column names
        df.columns = df.columns.fillna('')
        empty_name_cols = df.columns[df.columns.str.strip() == '']
        if len(empty_name_cols) > 0:
            #st.write(f"Detected columns with empty names: {list(empty_name_cols)}")
            df.drop(columns=empty_name_cols, inplace=True)
            #display_headers_info(df, "Data After Dropping Empty Column Names:")

    # Step 5: Standardize headers with GPT
    with st.spinner('Standardizing headers...'):
        instructions_prompt = standardization_instructions()
        original_headers = list(df.columns)
        standardized_headers = standardize_headers_with_retries(original_headers, instructions_prompt, client)
        standardized_headers = make_column_names_unique(standardized_headers)
        df.columns = standardized_headers
        display_headers_info(df, "DataFrame After GPT Header Standardization:")

    # Step 6: Drop unnecessary rows
    with st.spinner('Dropping unnecessary rows...'):
        df = drop_unnecessary_rows(df)
        #display_headers_info(df, "DataFrame After Dropping Unnecessary Rows:")

    # Step 7: Find and apply breaking point
    with st.spinner('Finding breaking point...'):
        breaking_point = find_breaking_point(df)
        if breaking_point is not None:
            df = df[:breaking_point].reset_index(drop=True)
            st.success(f"Applied breaking point at row: {breaking_point}")
        #display_headers_info(df, "Data After Breaking Point:")

    # Step 8: Finalize columns with date formatting
    with st.spinner('Finalizing columns...'):
        df = finalize_columns(df)
        display_headers_info(df, "Data After Finalizing Columns with Date Formatting:")

    # Process occupancy status
    st.write("### Step 2")
    st.write("Processing occupancy status...")
    with st.spinner('Processing occupancy status...'):
        try:
            property_name, as_of_date = get_property_info(file_buffer)
            df = refine_occupancy_status(df, max_workers=50, 
                                     property_name=property_name, as_of_date=as_of_date)
            
        except Exception as e:
            st.error(f"Error during occupancy status processing: {str(e)}")

    # Drop unnecessary columns
    st.write("Dropping Unnecessary ...")
    with st.spinner('Dropping unnecessary columns...'):
        try:
            df = refine_dataframe(df)
        except Exception as e:
            st.error(f"Error dropping columns: {str(e)}")

    # Save to Excel
    buffer = io.BytesIO()
    
    return df


#########################
# Example Streamlit App
#########################

def main():
    st.title("Rent Roll Standardization Demo")

    openai.api_key = st.secrets["OPENAI_API_KEY"]

    st.write("Upload an Excel file to process:")
    uploaded_file = st.file_uploader("Choose an .xlsx file", type=["xlsx"])

    if uploaded_file is not None:
        st.write("Processing file...")
        final_df = standardize_data_workflow(uploaded_file)

        if final_df is not None:
            st.success("Processing Complete!")
            st.dataframe(final_df)

            # Allow user to download results
            buffer = io.BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                final_df.to_excel(writer, index=False, sheet_name="Processed")
            download_data = buffer.getvalue()

            st.download_button(
                label="Download Processed Rent Roll",
                data=download_data,
                file_name="processed_rent_roll.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
    else:
        st.info("Awaiting file upload...")


if __name__ == "__main__":
    main()
