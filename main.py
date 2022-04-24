#!/usr/bin/env python3
# coding=utf-8
#
"""
COPOST POND project table manipulation
"""
# STATUS entries may be PENDING, ACTIVE (changed to PENDING at start of cycle), FLAGGED, INACTIVE, AND CHAPLIN

import pandas as pd
import re
from datetime import datetime
import dateutil

housing_re = re.compile(r'^HSG:|-\d| YARD$|^ZONE |^UNIT |^\d+$')  # recognize housing address
jurisdiction_res = {'NONE': re.compile(r'^$'),
                    'COUNTY': re.compile(r'JAIL|COUNTY'),
                    'FEDERAL': re.compile(r'FEDERAL|FMC|USP|FCI')}
FACILITY_COL = 3  # FACILITY column index in resident table
[JURISDICTION_COL, J_STATE_COL, TYPE_COL, WEB_LINK_COL] = range(4)  # col indices in facilities table
TIME_DISPLAY_FORMAT = '%-m/%-d/%-y'  # datetime output format
DEFAULT_TIME = '1/1/1970'
STATUS_MAP = {'': 'FLAGGED',
              'IN CUSTODY': 'PENDING',
              'CHAPLAIN': 'CHAPLAIN'}
COLUMN_RENAMING = {'I_Fname': 'FIRST_NAME',
                   'I_Lname': 'LAST_NAME',
                   'O_GS_Code': 'ORIGIN',
                   'CO_db_date': 'CO_DATE',
                   'O_Date': 'ORIGIN_DATE',
                   'I_InmateID': 'INMATE_ID',
                   'Release': 'RELEASE',
                   'Notes as of 10/3/21': 'NOTES'}

def add_note(df, row_index, source, note):
    """Append a note (if there is one) to NOTES field in the indicated row of df, indicating the source of the note."""
    if note:
        df.at[row_index, 'NOTES'] = f"{df.loc[row_index, 'NOTES']} {source}: {note}".strip()

def df_write_csv(name, df):
    """Write DF to NAME.csv"""
    with open(f'Data/{name}.csv', 'w') as f:
        df.to_csv(f)

def df_sort(df):
    """Return DF sorted on all columns"""
    return df.sort_values(list(df.columns))

def main(csv_file):
    """
    Read CSV_FILE: a COPOST resident address table.
    Create a Prisons.csv file table, moving the current address fields from the resident table, with a
    single entry for each facility. Replace resident table address information with a facility table
    record reference.

    Prisoner housing information, in some ADDRESS1 fields of the resident table, is moved to a new
    HOUSING field in the resident table.

    And other things; the above is not a complete list of actions.
    """
    # Read resident sheet as dataframe of resident sheet and do elementary cleanup
    with open(csv_file, newline='') as f:
        df = pd.read_csv(f, dtype=str, header=0)  # 0-based indices in first column, not including header
    # Not needed after cleanup
    # df = df.drop(labels=range(727, 733), axis=0).reset_index(drop=True)  # drop rows with random data
    df.fillna('', inplace=True)  # blank fields back to empty strings so string ops aren't fouled-up
    df = df.applymap(lambda s: s.strip().upper())  # strip leading and trailing spaces and uppercase for uniformity

    # Rename, save, drop and add columns
    df = df.rename(columns=COLUMN_RENAMING)
    df_saved_cols = df.loc[:, 'Sent Letter':'LAST_NAME']
    df = df.drop(df.loc[:, 'Sent Letter':'Full Name'].columns, axis=1)
    df = df.drop(['XXX'], axis=1)  # no data
    df = df.drop(df.loc[:, 'P_Name':'I_Zip'].columns, axis=1)  # drop PCF data we don't need
    status_col = df.pop('STATUS')
    df.insert(loc=df.columns.get_loc('NOTES'), column='SENT_LETTER', value='')
    df.insert(loc=df.columns.get_loc('NOTES'), column='STATUS', value=status_col)
    df.insert(loc=FACILITY_COL, column='FAC_INDEX', value=0)  # insert after I_InmateID
    df.insert(loc=FACILITY_COL + 1, column='HOUSING', value='')  # last known housing within facility (optional)
    df.insert(loc=FACILITY_COL + 2, column='LANGUAGE', value='English')
    df.insert(loc=df.columns.get_loc('CO_DATE') + 1, column='REPEAT', value=False)

    # Iterate over resident records
    for row_index in range(df.shape[0]):
        origin = str(df.iloc[row_index]['ORIGIN'])

        # LANGUAGE is Spanish when ORIGIN is FGW-S
        if origin == 'FGW-S':
            df.at[row_index, 'LANGUAGE'] = 'Spanish'

        # Change all PCF origin codes to PCF
        if origin in ['FGW-E', 'FGW-S', 'PCF temp']:
            df.at[row_index, 'ORIGIN'] = 'PCF'

        # Move HOUSING information to its new column
        if housing_re.search(df.iloc[row_index]['ADDRESS1']):
            df.at[row_index, 'HOUSING'] = df.iloc[row_index]['ADDRESS1']
            df.at[row_index, 'ADDRESS1'] = ''

        # Initial field values to variables, and process them
        sent_letter, checked, full_name, first_name, last_name = df_saved_cols.iloc[row_index, :].values.tolist()
        if not first_name:  # If no first name, fill first and last from full name
            name_parts = full_name.split()
            df.at[row_index, 'FIRST_NAME'] = ' '.join(name_parts[: -1])
            df.at[row_index, 'LAST_NAME'] = name_parts[-1]
        else:  # Check that full name (with multiple spaces reduced to one) is first name followed by last name
            make_full_name = f'{first_name} {last_name}'.strip()
            if re.sub(r'\s+', ' ', full_name) != make_full_name:
                print(f'Bad full name {full_name} not {make_full_name} index {row_index}')
                print(list(map(ord, list(full_name))))
                print(list(map(ord, list(make_full_name))))
        if sent_letter:
            df.at[row_index, 'SENT_LETTER'] = DEFAULT_TIME if sent_letter == 'COPOST' else sent_letter.split()[0]

        if checked != 'X':
            add_note(df, row_index, 'CHECKED', checked)

        # Reformat origin dates
        o_date = df.at[row_index, 'ORIGIN_DATE']
        if o_date:
            date = dateutil.parser.parse(o_date)
            df.at[row_index, 'ORIGIN_DATE'] = date.strftime(TIME_DISPLAY_FORMAT)

        # CO_DATE formatting and notes
        co_date = df.at[row_index, 'CO_DATE']
        if co_date:
            try:
                df.at[row_index, 'CO_DATE'] = datetime.strptime(co_date, '%m/%d/%Y').strftime(TIME_DISPLAY_FORMAT)
            except ValueError:  # Move entry to NOTES
                df.at[row_index, 'CO_DATE'] = ''
                add_note(df, row_index, 'CO_DATE', co_date)

        # Status reformatting and notes
        status = df.at[row_index, 'STATUS']
        new_status = STATUS_MAP.get(status, None)
        if not new_status:
            new_status = 'FLAGGED'
            add_note(df, row_index, 'STATUS', status)
        df.at[row_index, 'STATUS'] = new_status

    # Create df for facilities, moving address information from resident table
    df_a = df.loc[:, 'FACILITY':'ZIP']
    # df = df.drop(df.loc[:, 'FACILITY':'ZIP'].columns, axis=1)

    # Sort facilities, drop duplicates, and add fields at end
    df_as = df_sort(df_a)
    df_nd = df_sort(df_as.drop_duplicates())
    num_facility_cols = df_nd.shape[1]
    df_nd.insert(loc=num_facility_cols, column='CHECKED', value='')
    df_nd.insert(loc=num_facility_cols + 1, column='NOTES', value='')

    # Check that other address fields uniquely determine the zip code
    addresses = [' '.join(row[: -1]) for row in df_nd.to_numpy()]
    for i in range(len(addresses) - 1):
        if addresses[i] == addresses[i + 1]:
            print(f'Zip in row {i + 1} with address {addresses[i]} differs from following line')

    # Add JURISDICTION, J_STATE (initialized to STATE values), TYPE, and WEB LINK fields after INDEX
    df_nd.insert(loc=JURISDICTION_COL, column='JURISDICTION', value='STATE')
    df_nd.insert(loc=J_STATE_COL, column='J_STATE', value=df_nd['STATE'])
    df_nd.insert(loc=TYPE_COL, column='TYPE', value='')
    df_nd.insert(loc=WEB_LINK_COL, column='WEB_LINK', value='')

    # Capture the address-history indices
    ia_facilities = list(df_nd.index)
    ia_residents = list(df_as.index)

    # Iterate over facilities
    num_facilities = df_nd.shape[0]
    next_index = None
    for facility_index in range(num_facilities):
        # Get first and last resident indices corresponding to the current facility
        if facility_index < num_facilities - 1:
            this_id, next_id = ia_facilities[facility_index: facility_index + 2]
            this_index = ia_residents.index(this_id)
            next_index = ia_residents.index(next_id)
        else:
            this_index = next_index
            next_index = num_facilities - 1

        # Fill in FACILITY_COL of the residents table
        for resident in ia_residents[this_index: next_index]:
            df.iat[resident, FACILITY_COL] = facility_index

        facility = str(df_nd.iloc[facility_index]['FACILITY'])
        for jurisdiction, j_re in jurisdiction_res.items():
            if j_re.search(facility):
                df_nd.iat[facility_index, JURISDICTION_COL] = jurisdiction

                f_type = ''  # Not sure if we're doing this.  ('PRISON' if jurisdiction == 'FEDERAL'
                #           else 'JAIL' if jurisdiction == 'COUNTY'
                #           else '')
                break
        else:
            f_type = ''  # 'STATE'
        df_nd.iat[facility_index, TYPE_COL] = f_type

    # Write the facilities and residents tables
    df_nd = df_nd.reset_index()
    df_nd.index.name = 'INDEX'  # or remove index with df_nd = df_nd.reset_index(drop=True)
    df_write_csv('facilities', df_nd)
    df.index.name = 'INDEX'  # or remove index with df = df.reset_index(drop=True)
    df_write_csv('residents', df)


# Invoke main if executing as a script (not module load)
if __name__ == '__main__':
    # numpy_matrix_test()
    main('Data/SEND TO THESE.csv')
