import pandas as pd
from datetime import datetime
import numpy as np


def newtransactioncolumn(row, val1, val2):
    '''it takes a row and 2 value as arguments
    return either of the value depeding on the condition on row'''
    if pd.isna(row[0]) & pd.notna(row[1]):
        if row[1] > 0:
            return val2
        else:
            raise ValueError('value is less than 0 error in file')
    elif pd.notna(row[0]) & pd.isna(row[1]):
        if row[0] > 0:
            return val1
        else:
            raise ValueError('value is less than 0 error in file')
    else:
        raise Exception('error in file')


def group_and_save(df, gby, getg):
    '''df = dataframe
    gby = column to apply group by
    getg = columns to get group'''
    if type(gby) != list:
        raise Exception('group by should be provided in list')
    if pd.Series(gby).isin(df.columns).all():
        new_group = df.groupby(gby)
        return new_group.get_group(getg)
    else:
        raise Exception('group by column doesnt exist in dataframe')


def data_in_range(df, clmn, start, end):
    '''df = dataframe
    clmn = column on which condition is to be applied
    start = start date
    end = end date
    filename = name to save file'''
    startdate = pd.to_datetime(start, format='%Y-%m-%d')
    enddate = pd.to_datetime(end, format='%Y-%m-%d')
    # startdate = datetime.strptime(start, '%Y-%m-%d')
    # enddate = datetime.strptime(end, '%Y-%m-%d')
    fltr = (df[clmn] >= pd.to_datetime(startdate)) & (
        df[clmn] <= pd.to_datetime(enddate))
    return df.loc[fltr]


def aggondate(start, end, df, clmn, gby, applyon):
    '''start = start date
    end = end date
    df = dataframe
    clmn = column on which condition is to be applied
    gby = columns to apply group by
    applyon = columns to apply aggregate metgod'''
    # startdate = datetime.strptime(start, '%Y-%m-%d')
    # enddate = datetime.strptime(end, '%Y-%m-%d')
    startdate = pd.to_datetime(start, format='%Y-%m-%d')
    enddate = pd.to_datetime(end, format='%Y-%m-%d')
    fltr = (df[clmn] >= startdate) & (df[clmn] <= enddate)
    fdf = df.loc[fltr]
    grp = fdf.groupby(gby)
    return grp[applyon].sum()


def main():
    df = pd.read_excel('bank.xlsx', 'Sheet1')

    # topic 1.1
    df.drop(columns=['.'], inplace=True)

    # topic 1.2
    df['TransactionAmount'] = df[['WITHDRAWAL AMT', 'DEPOSIT AMT']].apply(
        lambda row: newtransactioncolumn(row, row['WITHDRAWAL AMT'], row['DEPOSIT AMT']), axis=1)

    # topic 1.3
    df['TransactionType'] = df[['WITHDRAWAL AMT', 'DEPOSIT AMT']].apply(
        lambda row: newtransactioncolumn(row, val1='DR', val2='CR'), axis=1)

    # topic 1.4
    df[['Account No', 'DATE', 'TRANSACTION DETAILS', 'CHQ.NO.', 'VALUE DATE', 'WITHDRAWAL AMT', 'DEPOSIT AMT',
        'TransactionType', 'TransactionAmount', 'BALANCE AMT']].to_csv('newbank.csv', index=False)

    df2 = pd.read_csv('newbank.csv')

    # topic 2.1
    fltr = (pd.isna(df2['CHQ.NO.']) == False)
    chqdf = df2.loc[fltr]
    chqdf.to_parquet('allchq.parquet', index=False)

    # topic 2.2
    filename = 'allcr'
    new_result = group_and_save(
        df2, ['TransactionType'], 'CR')
    new_result.to_parquet(f'{filename}.parquet', index=False)

    # topic 2.3
    filename = 'alldr'
    new_result = group_and_save(
        df2, ['TransactionType'], 'DR')
    new_result.to_parquet(f'{filename}.parquet', index=False)

    # topic 3
    df2['DATE'] = pd.to_datetime(df2['DATE'], format='%Y-%m-%d')
    start = '2019-10-12'
    end = '2019-10-12'
    filename = 'topic3'
    start = pd.to_datetime(start, format='%Y-%m-%d')
    end = pd.to_datetime(end, format='%Y-%m-%d')
    # start = datetime.strptime(start, '%Y-%m-%d')
    # end = datetime.strptime(end, '%Y-%m-%d')
    new_result = data_in_range(df2, 'DATE', start, end)
    new_result.to_csv(f'{filename}.csv', index=False)

    # topic 4

    df3 = pd.DataFrame()
    df3[['Account No', 'DATE', 'TransactionType', 'TransactionAmount']
        ] = df2[['Account No', 'DATE', 'TransactionType', 'TransactionAmount']]
    df3['count'] = 1
    fltr = df3.duplicated()
    topic4 = df3.loc[fltr].groupby(
        ['Account No', 'DATE', 'TransactionType', 'TransactionAmount']).sum()
    topic4.to_csv('topic4.csv')

    # topic 5
    start = '2019-03-05'
    end = '2019-06-05'
    newdata = aggondate(start, end, df2, 'DATE', ['Account No'], ['DEPOSIT AMT', 'WITHDRAWAL AMT']).rename(
        columns={'DEPOSIT AMT': 'TOTAL DEPOSIT', 'WITHDRAWAL AMT': 'TOTAL WITHDRAWAL'})
    newdata.to_csv('agg.csv')

    print('completed...')


if __name__ == '__main__':
    main()
