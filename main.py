#!/usr/bin/env python3
# coding=utf-8
#
"""
COPOST POND project table manipulation
"""

import pandas as pd
import re

housing_re = re.compile(r'^HSG:|-\d| YARD$|^ZONE |^UNIT |^\d.$')
PRISON_COL = 2  # PRISON column index in prison table

def df_write_csv(name, df):
    """Write DF to NAME.csv"""
    with open(f'Output/{name}.csv', 'w') as f:
        df.to_csv(f)

def df_write_txt(name, df):
    """Write DF to Output/NAME.txt"""
    with open(f'Output/{name}.txt', 'w') as f:
        # Attempt to left align everything XX
        # left_aligned_df = df.style.set_properties(**{'text-align': 'left'})
        # left_aligned_df = left_aligned_df.set_table_styles(
        #     [dict(selector='th', props=[('text-align', 'left')])]
        # )
        # f.write(left_aligned_df)
        # f.write(dfs.to_string(justify='left'))
        f.write(df.to_string())

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

    # Move HOUSING information to its new column
    for row_index in range(1, df.shape[0]):
        if housing_re.search(df.iloc[row_index]['ADDRESS1']):
            df.at[row_index, 'HOUSING'] = df.iloc[row_index]['ADDRESS1']
            df.at[row_index, 'ADDRESS1'] = ''

    # Create df for prisons, moving address information from prisoner table
    df_a = df.loc[:, 'FACILITY':'ZIP']
    df_write_txt('prisoners_1', df)  # XX
    df = df.drop(columns=list(df_a.columns.values) + ['XXX'])

    # Sort prisons and drop duplicates
    df_as = df_sort(df_a)
    df_write_txt('addresses_sorted', df_as)  # XX
    df_nd = df_sort(df_as.drop_duplicates())
    df_write_txt('prisons', df_nd)  # XX
    df_write_csv('prisons', df_nd)

    # Fill in PRISONERS field lists and fill in prisoners table PRISON fields
    ia_prisons = list(df_nd.index)  # capture the address-history indices
    # XX df_nd = df_nd.reset_index(drop=True)  # only then reset the indices for table assignment
    ia_prisoners = list(df_as.index)
    print(ia_prisons)  # xx
    print(ia_prisoners)  # xx
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
        print(prison_index, ia_prisoners[this_index: next_index])  # XX
        for prisoner in ia_prisoners[this_index: next_index]:
            df.iloc[prisoner, PRISON_COL] = prison_index
    df_write_txt('prisoners', df)  # XX
    df_write_csv('prisoners', df)


# Initiate run if executing as a script (not module load)
if __name__ == '__main__':
    # numpy_matrix_test()
    main('Input/PrisonerAddresses.csv')
