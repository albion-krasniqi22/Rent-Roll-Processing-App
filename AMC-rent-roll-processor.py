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

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from datetime import datetime

DRIVE_FOLDER_ID = "1vg2uwp8PmeZ-pivoolRR25gj5Hdwmufm"


client = openai.OpenAI(api_key=st.secrets["OPENAI_API_KEY"])

def display_headers_info(df, message=""):
    """
    Print a message and then show a few rows to see the headers visually (in Streamlit).
    """
    if message:
        st.write(message)
    st.dataframe(df.head(5))
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
    except Exception as excel_error:
        try:
            # If Excel fails, try reading as standard CSV
            df = pd.read_csv(filepath, header=None)
            return df
        except Exception as csv_error:
            st.error(f"Error reading Excel file: {excel_error}")
            return None

def read_top_rows(file_buffer, max_rows=10):
    """
    Reads the top 'max_rows' from the uploaded Excel file (no header)
    and converts them into a single text block.
    """
    df_top = load_excel_file(file_buffer)
    if df_top is None:
        return None
        
    # Take only the first max_rows
    df_top = df_top.head(max_rows)
    
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
    normalized_data = sheet_data.applymap(lambda x: str(x).lower() if pd.notnull(x) else '')
    
    normalized_data['keyword_count'] = normalized_data.apply(
        lambda row: sum(row.str.contains('|'.join(keywords), regex=True)), axis=1
    )
    
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
        - "Unit", "Unit Id", "Unit Number", "bldg-unit", "apt #", "apt number", "Suite"
        - Columns containing the substring "Id" can be mapped to "Unit" only if no other "Unit"-related columns (e.g., "Unit", "Unit Number", etc.) are available.
        - Avoid "Unit No.": Clearly specifies that this rule applies only to the "Unit" column and not to "Unit No."., cols like Lease ID, Resh ID should not be Unit No.
    - Floor Plan Code: Includes variations like "Floor Plan", "Plan Code", "Floorplan", "Unit Type", Bd/Ba, "Type"
    - Net sf: Includes variations like "Sqft", "Unit Sqft", "Square Feet", "Sq. Ft."
    - Occupancy Status / Code: Includes variations like "Unit Status", "Lease Status", "Occupancy", "Unit/Lease Status"
    - Market Rent: Includes variations like "Market Rent", "Market + Addl.", "Gross Market Rent", "Market", "Target Rent"
    - Rent: when it is only Rent or lease rent
    - Lease Start Date: Includes variations like "Lease Start", "Lease Start Date", "Start of Lease" (not lease name)
    - Lease Expiration: Includes variations like "Lease End", "Lease End Date", "Lease Expiration Date"
    - Move In Date: Includes variations like "Move-In", "Move In Date", "Move In"
    - Move Out Date: Includes variations like "Move-Out", "Move Out Date", "Move Out", "Notice"
    - Charge Codes: Includes variations like "Trans Code", "Charge Codes", "Description", "Lease Charges", "Codes"
    - Amount: these are charges in dollar amount (which is different from charge code), "Values", "Lease Rent", "Charges or credits", "Scheduled Charges", and "Actual charges" (do not rename Recurring Charges, Monthly Charges, Monthly Rent).

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
            counts[col] = 0  # Initialize counter for the first occurrence

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

    charge_columns = ['Charge Codes', 'Amount']
    other_columns = [col for col in df.columns if col not in charge_columns]

    empty_unit_mask = df['Unit No.'].isna()

    # For rows with empty Unit No., set all other columns (except charge_columns) to NaN
    for col in other_columns:
        if col != 'Unit No.':  
            df.loc[empty_unit_mask, col] = np.nan

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

            for rent_col in rent_columns:
                if pd.isnull(row.get(rent_col)):
                    data.at[index, rent_col] = 0

            if not (
                (
                    'Net sf' not in row or 
                    pd.isnull(net_sf) or  # Allow net_sf to be empty
                    (pd.notnull(net_sf) and net_sf < 5000)
                ) and
                (
                    any(
                        pd.notnull(row[col]) and float(str(row[col]).replace(',', '')) < 10000
                        for col in rent_columns
                    ) or (lease_start_exists and pd.notnull(row.get('Lease Start Date')))
                )
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
        "Leave Column Blank"
    ]

    # 0) Filter out rows that lack a valid "Unit No."
    #    i.e., exclude NaN or empty string
    if "Unit No." not in df.columns:
        print("No 'Unit No.' column found. Cannot proceed with grouping or pivoting.")
        return df

    # 1) Detect if we have the columns needed to pivot charges
    has_charge_codes = ("Charge Codes" in df.columns and "Amount" in df.columns)

    if not has_charge_codes and "Amount" in df.columns:
        desired_columns.append("Amount")
        print("Single-line scenario detected. Preserving Amount column.")
        
    # 2) Define columns to combine into the unique key
    intersection_cols = [c for c in df.columns 
                        if c not in ("Charge Codes", "Amount") 
                        and c in desired_columns]
    group_cols = intersection_cols
    # ------------------------------------------------------------------------
    if "Amount" in df.columns:
        df['Amount'] = df['Amount'].astype(str).str.replace('$', '').str.replace(',', '').str.strip()
        df['Amount'] = pd.to_numeric(df['Amount'], errors='coerce')

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

    # For single-line scenarios, include Amount in the aggregations
    if not has_charge_codes and "Amount" in df.columns:
        aggregations["Amount"] = "first"

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

    # Drop 'unique_key' if it exists
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
                .dt.strftime("%m/%d/%Y")
            )
    final_df = final_df.dropna(subset=["Unit No."]).reset_index(drop=True)
    return final_df

def drop_total_rows(df):
    """
    Drop any rows that contain variations of the word 'total' in any column.
    Returns the DataFrame with those rows removed.
    """
    # Convert all values to string and lowercase for comparison
    df_str = df.astype(str).apply(lambda x: x.str.lower())
    
    # Define variations of 'total' to look for
    total_variations = ['total', 'totals', 'subtotal', 'sub-total', 'sub total']
    
    # Create a mask that identifies rows containing any variation of 'total'
    total_mask = df_str.apply(
        lambda x: ~x.str.contains('|'.join(total_variations), na=False)
    ).all(axis=1)
    
    # Apply the mask and reset index
    cleaned_df = df[total_mask].reset_index(drop=True)
    
    # Log how many rows were removed
    rows_removed = len(df) - len(cleaned_df)
    if rows_removed > 0:
        print(f"Removed {rows_removed} rows containing variations of 'total'")
    
    return cleaned_df
    
def process_row(row):
    """Process a single row and determine occupancy status."""
    # Prepare the row data as a string for the API
    row_data = row.to_dict()
    row_text = "\n".join([f"{key}: {value}" for key, value in row_data.items()])
    
    # Prompt for API
    prompt = f"""
        You are a data processing assistant. I will provide you with rows of data from a CSV file, each representing information about a rental unit. Your task is to categorize the Occupancy Status / Code for each row based on these refined rules:
        Occupied: The row contains a tenant name, and at least one of the following fields: lease start date, lease expiration date, or move-in date. Additional charges may also be present along with the market rent. If the status indicates “current” or “notice,” it is considered as Occupied.
        Vacant: The tenant name is absent or displays ‘Vacant.’ Lease-related fields (dates) are empty. Market rent is present, but no other charges exist.
        Model: The tenant name displays ‘Model,’ or the row explicitly indicates it is an administrative unit. Market rent is present, but no other charges or lease-related information are included.
        Priority Rule: If both Model and Vacant are present, classify the row as Model.
        Admin: If the row explicitly states it is an administrative unit (e.g. Admin/Down, Admin) but does not meet the criteria for any of the above categories, classify it as Admin. If both Admin and Vacant are present, classify the row as Admin.
        Applicant: A tenant name is present, and lease-related fields (e.g., dates) may be present. Only market rent is listed; no other charges are included.  The row may explicitly mention “Applicant.”
        Special Cases: Rows marked as “pending renewals” are also classified as Applicant.
        
        Based on the following data, determine the Occupancy Status / Code: 
        {row_text}
        Only return the category as the output (Occupied, Vacant, Model, Admin, or Applicant).
        """
    
    # Call the OpenAI API
    try:
        response = client.chat.completions.create(
            model="o3-mini",
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
        Reference Date: {as_of_date} if this is None take today's date as a placeholder.

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

        Only return the category as the output (Occupied, Vacant, Applicant).
        """
    
    try:
        response = client.chat.completions.create(
            model="o3-mini",
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



def refine_pairs_parallel(df, pairs, property_name, as_of_date, max_retries=3, max_workers=100):
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

    applicant_mask = df['Occupancy Status / Code'] == 'Applicant'
    
    # Split the DataFrame into non-Applicant and Applicant rows
    non_applicant_df = df[~applicant_mask].copy()
    applicant_df = df[applicant_mask].copy()
    
    # Concatenate the DataFrames with Applicant rows at the bottom
    df = pd.concat([non_applicant_df, applicant_df], ignore_index=True)

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
        - Do not drop columns such as comrent keep, future lease (F), concessions, write off, amount/amounts, Leave Blank

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


def get_drive_service():
    """Initialize and return Google Drive service"""
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

def add_metadata_rows(df, property_name, as_of_date):
    """Add property name and as-of date before the column headers"""
    # Get column headers
    headers = df.columns.tolist()
    
    # Create the data for the Excel file
    data = []
    
    # Add metadata rows (combined label and value in first cell)
    data.append([f'Property Name: {property_name}'] + [''] * (len(headers) - 1))
    data.append([f'As of Date: {as_of_date}'] + [''] * (len(headers) - 1))
    
    # Add column headers
    data.append(headers)
    
    # Add the DataFrame data
    data.extend(df.values.tolist())
    
    # Create new DataFrame without headers (they're now in the data)
    final_df = pd.DataFrame(data)
    
    return final_df

def check_file_exists(service, filename):
    """Check if file already exists in the Drive folder"""
    try:
        response = service.files().list(
            q=f"name='{filename}' and '{DRIVE_FOLDER_ID}' in parents and trashed=false",
            spaces='drive',
            fields='files(id, name)'
        ).execute()
        
        files = response.get('files', [])
        return files[0]['id'] if files else None
        
    except Exception as e:
        st.error(f"Error checking file existence: {str(e)}")
        return None

def upload_to_drive(buffer, original_filename, property_name=None, as_of_date=None):
    """Upload or update file in Google Drive"""
    try:
        service = get_drive_service()
        
        # Get the base filename and extension
        base_name = original_filename.rsplit('.', 1)[0]
        ext = '.xlsx'
        final_filename = f"{base_name}_processed{ext}"
        
        # Check if file already exists
        existing_file_id = check_file_exists(service, final_filename)
        
        if existing_file_id:
            # Update existing file
            file_metadata = {'name': final_filename}
            media = MediaIoBaseUpload(buffer, 
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True)
            
            file = service.files().update(
                fileId=existing_file_id,
                body=file_metadata,
                media_body=media
            ).execute()
            
            return True, f"File updated successfully with ID: {file.get('id')}"
        else:
            # Create new file
            file_metadata = {
                'name': final_filename,
                'parents': [DRIVE_FOLDER_ID]
            }
            
            media = MediaIoBaseUpload(buffer,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True)
            
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            return True, f"New file uploaded successfully with ID: {file.get('id')}"
    
    except Exception as e:
        return False, f"Error uploading to Drive: {str(e)}"

def upload_original_to_drive(file_buffer):
    """Upload or update original file in Google Drive"""
    try:
        service = get_drive_service()
        original_filename = file_buffer.name
        
        # Check if file already exists
        existing_file_id = check_file_exists(service, original_filename)
        
        if existing_file_id:
            # Update existing file
            file_metadata = {'name': original_filename}
            media = MediaIoBaseUpload(file_buffer,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True)
            
            file = service.files().update(
                fileId=existing_file_id,
                body=file_metadata,
                media_body=media
            ).execute()
            
            return True, f"Original file updated successfully with ID: {file.get('id')}"
        else:
            # Create new file
            file_metadata = {
                'name': original_filename,
                'parents': [DRIVE_FOLDER_ID]
            }
            
            media = MediaIoBaseUpload(file_buffer,
                mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                resumable=True)
            
            file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            
            return True, f"Original file uploaded successfully with ID: {file.get('id')}"
    
    except Exception as e:
        return False, f"Error uploading original file: {str(e)}"


def standardize_data_workflow(file_buffer):
    with st.spinner('Uploading original file...'):
        # Reset file buffer position
        file_buffer.seek(0)
        success, message = upload_original_to_drive(file_buffer)
        if success:
            st.success("✅ " + message)
        else:
            st.error("❌ " + message)
    
    # Reset file buffer position for processing
    file_buffer.seek(0)
    
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
        
        # Clean column names
        df.columns = df.columns.fillna('')
        
        # 1) Find the first occurrence of 5+ consecutive unnamed columns
        consecutive_count = 0
        cutoff_index = None
        
        for idx, col in enumerate(df.columns):
            if col.strip() == '':
                consecutive_count += 1
                if consecutive_count >= 10 and cutoff_index is None:
                    # Mark where the run of 10 unnamed columns started
                    cutoff_index = idx - 9  
            else:
                consecutive_count = 0
        
        # 2) If we found 10+ consecutive unnamed columns, drop everything from that start onward
        if cutoff_index is not None:
            df = df.iloc[:, :cutoff_index]
            st.write(f"Dropped all columns at/after index {cutoff_index} because we found 10+ consecutive unnamed columns.")

        # 3) Now drop any remaining unnamed columns individually
        empty_name_cols = df.columns[df.columns.str.strip() == '']
        if len(empty_name_cols) > 0:
            df.drop(columns=empty_name_cols, inplace=True)
            st.write("Dropped remaining unnamed columns.")

    with st.spinner('Pre-processing special columns...'):
        df = pre_process_special_columns(df)
        st.write("Data after pre-processing special columns:")
        st.dataframe(df.head(5))


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

    # Step 6: Drop rows containing 'total'
    with st.spinner('Dropping total rows...'):
        df = drop_total_rows(df)

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
        st.write(f"Total Rows: {len(df)}")
        unique_units = df["Unit No."].nunique()
        st.write(f"Unique Units: {unique_units}")
        display_headers_info(df, "Data After Finalizing Columns with Date Formatting:")


    # Process occupancy status
    st.write("### Step 2")
    st.write("Processing occupancy status...")
    with st.spinner('Processing occupancy status...'):
        try:
            property_name, as_of_date = get_property_info(file_buffer)
            df = refine_occupancy_status(df, max_workers=500, 
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

    # Save and upload processed file
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
        df = add_metadata_rows(df, property_name, as_of_date)
        df.to_excel(writer, index=False, sheet_name="Processed", header=False)
    
    # Rewind buffer
    buffer.seek(0)
    
    # Upload to Google Drive
    with st.spinner('Uploading processed file...'):
        original_filename = file_buffer.name
        success, message = upload_to_drive(
            buffer, 
            original_filename,
            property_name=property_name,
            as_of_date=as_of_date
        )
        
        if success:
            st.success("✅ " + message)
        else:
            st.error("❌ " + message)
    
    buffer.seek(0)
    return df

def generate_observations(data_df, as_of_date):
    # Convert as_of_date to datetime
    try:
        as_of_date = pd.to_datetime(as_of_date)
    except:
        as_of_date = pd.Timestamp.today()

    # Prepare data summary for GPT
    summary = {
        "date_issues": [],
        "rent_issues": [],
        "sqft_issues": []
    }

    # Date analysis
    date_columns = {
        'Lease Start Date': 'lease start',
        'Move In Date': 'move-in',
        'Lease Expiration': 'lease end',
        'Move Out Date': 'move-out'
    }

    for col, desc in date_columns.items():
        if col in data_df.columns:
            dates = pd.to_datetime(data_df[col], errors='coerce')
            if desc in ['lease start', 'move-in']:
                future_dates = data_df[dates > as_of_date]
                if not future_dates.empty:
                    units = future_dates['Unit No.'].tolist()
                    summary['date_issues'].append(f"{len(units)} units have {desc} dates after {as_of_date.strftime('%Y-%m-%d')}: {', '.join(map(str, units[:5]))}")
            elif desc in ['lease end', 'move-out']:
                past_dates = data_df[dates < as_of_date]
                if not past_dates.empty:
                    units = past_dates['Unit No.'].tolist()
                    summary['date_issues'].append(f"{len(units)} units have {desc} dates before {as_of_date.strftime('%Y-%m-%d')}: {', '.join(map(str, units[:5]))}")

    # Rent analysis
    market_rent_col = next((col for col in data_df.columns if 'market rent' in col.lower()), None)
    if market_rent_col:
        # Clean market rent values by removing commas and converting to numeric
        market_rents = data_df[market_rent_col].astype(str).str.replace(',', '').str.replace('$', '')
        market_rents = pd.to_numeric(market_rents, errors='coerce')
        
        median_rent = market_rents.median()
        low_threshold = median_rent * 0.6
        high_threshold = median_rent * 1.4

        # Check for missing or negative rents
        missing_rent = data_df[market_rents.isna()]['Unit No.'].tolist()
        if missing_rent:
            summary['rent_issues'].append(f"{len(missing_rent)} units have no market rent reported: {', '.join(map(str, missing_rent[:5]))}")

        negative_rent = data_df[market_rents < 0]['Unit No.'].tolist()
        if negative_rent:
            summary['rent_issues'].append(f"{len(negative_rent)} units have negative market rent: {', '.join(map(str, negative_rent[:5]))}")

        # Check for outlier rents
        low_rent = data_df[market_rents < low_threshold]['Unit No.'].tolist()
        if low_rent:
            summary['rent_issues'].append(f"{len(low_rent)} units have market rent below 60% of median (${median_rent:.2f}): {', '.join(map(str, low_rent[:5]))}")

        high_rent = data_df[market_rents > high_threshold]['Unit No.'].tolist()
        if high_rent:
            summary['rent_issues'].append(f"{len(high_rent)} units have market rent above 140% of median (${median_rent:.2f}): {', '.join(map(str, high_rent[:5]))}")

    # Square footage analysis
    sqft_col = next((col for col in data_df.columns if 'net sf' in col.lower()), None)
    if sqft_col:
        # Clean square footage values by removing commas
        sqft = data_df[sqft_col].astype(str).str.replace(',', '')
        sqft = pd.to_numeric(sqft, errors='coerce')
        
        missing_sqft = data_df[sqft.isna()]['Unit No.'].tolist()
        if missing_sqft:
            summary['sqft_issues'].append(f"{len(missing_sqft)} units have no square footage reported: {', '.join(map(str, missing_sqft[:5]))}")

        negative_sqft = data_df[sqft < 0]['Unit No.'].tolist()
        if negative_sqft:
            summary['sqft_issues'].append(f"{len(negative_sqft)} units have negative square footage: {', '.join(map(str, negative_sqft[:5]))}")

    # Format the analysis
    analysis = ["1. Date Consistency Checks:"]
    if summary['date_issues']:
        analysis.extend(f"- {issue}" for issue in summary['date_issues'])
    else:
        analysis.append("- No date consistency issues found")

    analysis.extend(["", "2. Rent Analysis:"])
    if summary['rent_issues']:
        analysis.extend(f"- {issue}" for issue in summary['rent_issues'])
    else:
        analysis.append("- No rent anomalies found")

    analysis.extend(["", "3. Square Footage Analysis:"])
    if summary['sqft_issues']:
        analysis.extend(f"- {issue}" for issue in summary['sqft_issues'])
    else:
        analysis.append("- No square footage issues found")

    return "\n".join(analysis)


def main():
    st.title("AMC Rent Roll Standardization")

    openai.api_key = st.secrets["OPENAI_API_KEY"]

    # Initialize session state for processed data
    if 'processed_df' not in st.session_state:
        st.session_state.processed_df = None

    st.write("Upload an Excel file to process:")
    uploaded_file = st.file_uploader("Choose an .xlsx file", type=["xlsx", "xls", 'csv'])

    if uploaded_file is not None and st.session_state.processed_df is None:
        st.write("Processing file...")
        st.session_state.processed_df = standardize_data_workflow(uploaded_file)

    
    if st.session_state.processed_df is not None:
        st.success("Processing Complete!")
        st.dataframe(st.session_state.processed_df)

        # Allow user to download results
        buffer = io.BytesIO()
        with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
            # Write the processed data to the first sheet
            st.session_state.processed_df.to_excel(writer, index=False, header=False, sheet_name="Processed")
            
            # Create summary statistics for the second sheet
            data_df = st.session_state.processed_df.iloc[2:].copy()
            data_df.columns = st.session_state.processed_df.iloc[2]
            data_df = data_df.iloc[1:].reset_index(drop=True)
            
            # Create summary sheet data
            summary_data = []
            
            # Add basic counts
            summary_data.extend([
                ["Summary Statistics"],
                [""],
                ["Basic Counts"],
                ["Total Rows", len(data_df)],
                ["Unique Units", data_df['Unit No.'].nunique()],
                [""]
            ])
            
            # Add occupancy distribution
            summary_data.extend([["Occupancy Status Distribution"], [""]])
            occupancy_counts = data_df['Occupancy Status / Code'].value_counts()
            summary_data.extend([["Status", "Count"]])
            for status, count in occupancy_counts.items():
                summary_data.append([status, count])
            summary_data.append([""])
            
            # Add rent summary
            summary_data.extend([["Rent Summary"], [""]])
            rent_cols = [col for col in data_df.columns if any(term in col.lower() for term in ['rent', 'fee'])]
            summary_data.extend([["Category", "Total"]])
            for col in rent_cols:
                try:
                    values = pd.to_numeric(data_df[col], errors='coerce')
                    total = values.sum()
                    summary_data.append([col, f"${total:,.2f}"])
                except Exception:
                    continue
            
            # Write summary statistics to second sheet
            summary_df = pd.DataFrame(summary_data)
            summary_df.to_excel(writer, index=False, header=False, sheet_name="Summary Statistics")
            
            # Adjust column widths in summary sheet
            worksheet = writer.sheets["Summary Statistics"]
            for idx, col in enumerate(worksheet.columns):
                max_length = 0
                for cell in col:
                    try:
                        if len(str(cell.value)) > max_length:
                            max_length = len(str(cell.value))
                    except:
                        pass
                adjusted_width = (max_length + 2)
                worksheet.column_dimensions[worksheet.cell(row=1, column=idx+1).column_letter].width = adjusted_width


            as_of_date = st.session_state.processed_df.iloc[1, 0].split(': ')[1]
            observations = generate_observations(data_df, as_of_date)

            observations_data = [
                ["Data Analysis Observations"],
                [""],
                *[[line.strip()] for line in observations.split('\n') if line.strip()]
            ]
            observations_df = pd.DataFrame(observations_data)
            observations_df.to_excel(writer, index=False, header=False, sheet_name="Observations")
            
            # Adjust column widths in observations sheet
            worksheet = writer.sheets["Observations"]
            max_length = max(len(str(cell[0])) for cell in observations_data)
            worksheet.column_dimensions['A'].width = min(max_length + 2, 100)  # Cap width at 100 characters

        download_data = buffer.getvalue()

        # Use original filename
        original_filename = uploaded_file.name
        base_name = original_filename.rsplit('.', 1)[0]
        processed_filename = f"{base_name}_processed.xlsx"

        st.download_button(
            label="Download Processed Rent Roll",
            data=download_data,
            file_name=processed_filename,
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )
    else:
        st.info("Awaiting file upload...")


import re
import pandas as pd
import numpy as np

def extract_date_from_text(text):
    """
    Look for a date pattern in the text.
    This regex looks for dates in formats like 11/01/2016 or 11-01-2016.
    Adjust the regex if your date formats vary.
    """
    # Look for dates with "/" or "-" as separators.
    pattern = r'(\d{1,2}[/-]\d{1,2}[/-]\d{2,4})'
    match = re.search(pattern, text)
    return match.group(1) if match else None

def process_name_column(df, name_col_candidates=["Name"]):
    """
    For the identified name column, extract any embedded date.
    The extracted date is placed into a new column "Moving Date" and removed
    from the original Name value.
    """
    # Identify the candidate name column (exact match, case-insensitive)
    name_col = None
    for col in df.columns:
        if col.strip().lower() in [s.lower() for s in name_col_candidates]:
            name_col = col
            break

    # If not found, return df unchanged.
    if name_col is None:
        return df

    moving_dates = []
    new_names = []
    for val in df[name_col]:
        if pd.isna(val):
            new_names.append(val)
            moving_dates.append(np.nan)
        else:
            val_str = str(val)
            date_found = extract_date_from_text(val_str)
            if date_found:
                # Remove the date (and any extra surrounding symbols) from the name.
                new_val = re.sub(r'[\(\-\s]*' + re.escape(date_found) + r'[\)\-\s]*', '', val_str)
                new_names.append(new_val.strip())
                moving_dates.append(date_found)
            else:
                new_names.append(val_str.strip())
                moving_dates.append(np.nan)
    df[name_col] = new_names
    df["Move In Date"] = moving_dates
    return df

def process_lease_dates_column(df, lease_date_col_candidates=["Lease Dates"]):
    """
    For a column that contains lease dates (for example, "02/01/2024 01/31/2025"),
    split the content into "Lease Start Date" and "Lease End Date".
    This example assumes that the two dates are separated by whitespace.
    """
    lease_date_col = None
    for col in df.columns:
        if col.strip().lower() in [s.lower() for s in lease_date_col_candidates]:
            lease_date_col = col
            break

    if lease_date_col is None:
        return df

    lease_start = []
    lease_end = []
    for val in df[lease_date_col]:
        if pd.isna(val):
            lease_start.append(np.nan)
            lease_end.append(np.nan)
        else:
            # Split on whitespace; adjust the delimiter if needed.
            parts = re.split(r'\s+', str(val).strip())
            if len(parts) >= 2:
                lease_start.append(parts[0])
                lease_end.append(parts[1])
            else:
                lease_start.append(parts[0])
                lease_end.append(np.nan)
    df["Lease Start Date"] = lease_start
    df["Lease Expiration Date"] = lease_end
    # Optionally, drop the original lease dates column.
    df = df.drop(columns=[lease_date_col])
    return df

def pre_process_special_columns(df):
    """
    Apply special pre-processing to the DataFrame:
      - Extract a date from the Name column (if present) into a new column "Moving Date".
      - Split a Lease Dates column (if present) into "Lease Start Date" and "Lease End Date".
    """
    df = process_name_column(df, name_col_candidates=["Name"])
    df = process_lease_dates_column(df, lease_date_col_candidates=["Lease Dates"])
    return df

if __name__ == "__main__":
    main()
