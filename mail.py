#!/usr/bin/env python3
# coding=utf-8
#
"""
COPOST POND project mailing file generation
"""

import pandas as pd

def df_write_xlsx(name, df, index=False):
    """Write DF to NAME.xlsx without index option."""
    df.to_excel(f'Data/{name}.xlsx', index=index)

def df_read_xlsx(da):
    """
    Read Data/data - DA - TAB.csv with header.
    Return is as a dataframe with NAN's reverted to empty strings.
    """
    df = pd.read_excel(f'Data/data - {da}.xlsx', dtype=str, header=0)
    df.fillna('', inplace=True)  # blank fields back to empty strings
    return df

def address_fn(row):
    """Merge HOUSING and ADDRESS1 column information"""
    housing, address1 = row['HOUSING'], row['ADDRESS1']
    return ' '.join([housing, address1]).strip()

def full_name_id_fn(row):
    return f"{row['FIRST_NAME']} {row['LAST_NAME']}, {row['INMATE_ID']}"

def main():
    """The main event"""
    # TODO NC.csv output: FIRST NAME	MIDDLE NAME	LAST NAME	INMATE NUMBER
    #   separate excel file for each facility to NC directory
    dfr = pd.concat([df_read_xlsx('Michael'), df_read_xlsx('Phil')], axis=0)
    df = pd.DataFrame()
    df = pd.concat([df, dfr.loc[:, 'RES_INDEX':'INMATE_ID']], axis=1)
    df['FULL_NAME_ID'] = dfr.apply(full_name_id_fn, axis=1)
    df = pd.concat([df, dfr['HOUSING']], axis=1)
    df = pd.concat([df, dfr.loc[:, 'FACILITY':'ZIP']], axis=1)
    df['ADDRESS1'] = dfr.apply(address_fn, axis=1)
    df = df.rename(columns={'ADDRESS1': 'HOUSING_ADDRESS1'})
    df = df.drop(['HOUSING'], axis=1)
    df_write_xlsx('mailing', df)


# Invoke main if executing as a script (not module load)
if __name__ == '__main__':
    main()
