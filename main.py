#!/usr/bin/env python3
# coding=utf-8
#
"""
COPOST POND project table manipulation
"""

# TODO Prisoners table
#  Create LANGUAGE field, with default English, and Spanish when O_GS_Code ends in -S
#  Fields of original info from Ray
#  Delete P_Name through I.Zip
#  O_GS_Code rename nto ORIGIN, with the following replacements
#  FKW.. and PCF temp -> PCF
#  Rename CO_db_date to CO_DATE
#  Rename O_Date to ORIGIN_DATE
#  Eventually, but not now, remove col A with origin index numbers
# TODO Prison table
#   Check if address fields less zip uniquely determine zip
#    Create JURISDICTION field
#   Value is County if Jail in FACILITY field, None if FACILITY is empty, otherwise State
#   Eventually, but not now, remove col A with origin index numbers

import pandas as pd
import re

housing_re = re.compile(r'^HSG:|-\d| YARD$|^ZONE |^UNIT |^\d+$')
PRISON_COL = 2  # PRISON column index in prison table

def df_write_csv(name, df):
    """Write DF to NAME.csv"""
    with open(f'Output/{name}.csv', 'w') as f:
        df.to_csv(f)

def df_sort(df):
    """Return DF sorted on all columns"""
    return df.sort_values(list(df.columns))

def main(csv_file):
    """
    Read CSV_FILE: a COPOST prisoner address table.
    Create a Prisons.csv file table, moving the current address fields from the prisoner table, with a
    single entry for each prison. Replace prisoner table address information with a prison table
    record reference.

    Prisoner housing information, in some ADDRESS1 fields of the prisoner table, is moved to a new
    HOUSING field in the prisoner table.
    """
    # Create dataframe of prisoner table and add PRISON and HOUSING columns
    with open(csv_file, newline='') as f:
        df = pd.read_csv(f, dtype=str, header=0)  # 0-based indices in first column, not including header
    df = df.fillna('').applymap(lambda s: s.strip().upper())
    df.insert(loc=PRISON_COL, column='PRISON', value=0)  # insert after I_InmateID
    df = df.astype({'PRISON': int})
    df.insert(loc=PRISON_COL + 1, column='HOUSING', value='')  # last known housing within prison (optional)
    # df.insert(loc=PRISON_COL + 2, column='LANGUAGE', value='English')
    # df = df.rename(columns={'CO_db_date': 'CO_DATE', 'O_Date': 'ORIGIN_DATE'})

    # Move HOUSING information to its new column
    for row_index in range(1, df.shape[0]):
        if housing_re.search(df.iloc[row_index]['ADDRESS1']):
            df.at[row_index, 'HOUSING'] = df.iloc[row_index]['ADDRESS1']
            df.at[row_index, 'ADDRESS1'] = ''

    # Create df for prisons, moving address information from prisoner table
    df_a = df.loc[:, 'FACILITY':'ZIP']
    df = df.drop(columns=list(df_a.columns.values) + ['XXX'])

    # Sort prisons and drop duplicates
    df_as = df_sort(df_a)
    df_nd = df_sort(df_as.drop_duplicates())
    df_write_csv('prisons', df_nd)

    # Fill in PRISONERS field lists and fill in prisoners table PRISON fields
    ia_prisons = list(df_nd.index)  # capture the address-history indices
    ia_prisoners = list(df_as.index)
    num_prisons = df_nd.shape[0]
    next_index = None
    for prison_index in range(1, num_prisons):
        if prison_index < num_prisons - 1:
            this_id, next_id = ia_prisons[prison_index: prison_index + 2]
            this_index = ia_prisoners.index(this_id)
            next_index = ia_prisoners.index(next_id)
        else:
            this_index = next_index
            next_index = num_prisons - 1
        for prisoner in ia_prisoners[this_index: next_index]:
            df.iloc[prisoner, PRISON_COL] = prison_index
    df_write_csv('prisoners', df)


# Initiate run if executing as a script (not module load)
if __name__ == '__main__':
    # numpy_matrix_test()
    main('Input/PrisonerAddresses.csv')