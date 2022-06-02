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
    # noinspection PyArgumentList
    df = pd.read_excel(f'Data/data - {da}.xlsx', dtype=str, header=0)
    df.fillna('', inplace=True)  # blank fields back to empty strings
    return df

def address_fn(row):
    """Merge HOUSING and ADDRESS1 column information"""
    housing, address1 = row['HOUSING'], row['ADDRESS1']
    return ' '.join([housing, address1]).strip()

def full_name_id_fn(row):
    """Combine first and last name, followed by inmate ID."""
    return f"{row['FIRST_NAME']} {row['LAST_NAME']}, {row['INMATE_ID']}"


COLUMNS = 'FIRST NAME,MIDDLE NAME,LAST NAME,INMATE NUMBER'.split(',')
def write_facility(facility, f_rows):
    """Write a facility file"""
    if facility:
        df_write_xlsx(f'NC/{facility}', pd.DataFrame(f_rows, columns=COLUMNS))

def main():
    """Read data analyst resident Excel-format sheets and output mailing and NC sheets."""
    dfr = pd.concat([df_read_xlsx('Michael'), df_read_xlsx('Phil')], axis=0)  # Combine all data analyst resident sheets
    dfr = dfr.applymap(lambda s: s.strip().upper())  # Strip leading and trailing spaces and uppercase for uniformity
    dfr = dfr.loc[dfr['ACTIVE'] != 'I']  # Remove inactive records
    df = pd.DataFrame()  # Accumulate mailing columns in new df
    df = pd.concat([df, dfr.loc[:, 'RES_INDEX':'INMATE_ID']], axis=1)
    df['FULL_NAME_ID'] = dfr.apply(full_name_id_fn, axis=1)
    df = pd.concat([df, dfr['HOUSING']], axis=1)
    df = pd.concat([df, dfr.loc[:, 'FACILITY':'ZIP']], axis=1)
    df['ADDRESS1'] = dfr.apply(address_fn, axis=1)
    df = df.rename(columns={'ADDRESS1': 'HOUSING_ADDRESS1'})
    df = df.drop(['HOUSING'], axis=1)
    dfw = df[dfr['F_STATE'] != 'NC']  # don't write NC records
    df_write_xlsx('mailing', dfw)

    # Write NC/<facility>.xlsx for each facility in NC
    df = df[dfr['F_STATE'] == 'NC']  # only NC records
    df = df.sort_values(by=['FACILITY'])
    df = df.drop(df.loc[:, 'HOUSING_ADDRESS1':'ZIP'].columns, axis=1).drop(['RES_INDEX'], axis=1)
    facility, f_rows = None, None
    for _, row in df.iterrows():
        if facility != row['FACILITY']:
            write_facility(facility, f_rows)
            facility, f_rows = row['FACILITY'], []
        first_name = row['FIRST_NAME']
        if ' ' in first_name:
            first_name, middle_name = first_name.split(' ', maxsplit=1)
        else:
            middle_name = ''
        f_rows.append([first_name, middle_name, row['LAST_NAME'], row['INMATE_ID']])
    write_facility(facility, f_rows)


# Invoke main if executing as a script (not module load)
if __name__ == '__main__':
    main()
