#!/usr/bin/env python3
# coding=utf-8
#
"""
COPOST POND project mailing file generation
"""

import pandas as pd

def df_write_csv(name, df):
    """Write DF to NAME.csv without index"""
    with open(f'Data/{name}.csv', 'w') as f:
        df.to_csv(f, index=False)

def df_read_csv(da, tab):
    """
    Read Data/data - DA - TAB.csv with header.
    Return is as a dataframe with NAN's reverted to empty strings.
    """
    with open(f'Data/data - {da} - {tab}.csv') as f:
        df = pd.read_csv(f, dtype=str, header=0)
        df.fillna('', inplace=True)  # blank fields back to empty strings
        return df

def address_fn(row):
    """Merge HOUSING and ADDRESS1 column information, reporting any conflicts."""
    housing, address1 = row['HOUSING'], row['ADDRESS1']
    if housing and address1:
        print(f'RES_INDEX {row["RES_INDEX"]} has conflict of ADDRESS1 "{address1}" and HOUSING "{housing}"')
        return f'{housing} {address1}'
    else:
        return housing + address1

def main():
    """The main event"""
    # TODO NC.csv output: FIRST NAME	MIDDLE NAME	LAST NAME	INMATE NUMBER
    #   separate excel file for each facility to NC directory
    # TODO output .xlsx instead of .csv: df1.to_excel("output.xlsx")
    # TODO columns
    # RES_INDEX
    # FIRST_NAME
    # LAST_NAME
    # INMATE_ID
    # FULL_NAME_ID
    # FACILITY
    # HOUSING_ADDRESS1
    # ADDRESS2
    # CITY
    # STATE
    # ZIP
    # ?? dff = pd.concat([df_read_csv('Michael', 'facilities'), df_read_csv('Phil', 'facilities')], axis=0)
    dfr = pd.concat([df_read_csv('Michael', 'residents'), df_read_csv('Phil', 'residents')], axis=0)
    df = pd.DataFrame()
    df['Full Name'] = dfr[['FIRST_NAME', 'LAST_NAME']].agg(' '.join, axis=1)
    df['INMATE_ID'] = dfr['INMATE_ID']
    df = pd.concat([df, dfr.loc[:, 'FACILITY':'ZIP']], axis=1)
    df['ADDRESS1'] = dfr.apply(address_fn, axis=1)
    df_write_csv('for Pamela', df)


# Invoke main if executing as a script (not module load)
if __name__ == '__main__':
    main()
