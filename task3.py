import pandas as pd
import chardet
import re
from sqlalchemy import create_engine


def read_csv_file(path):
    with open(path, 'rb') as f:
        result = chardet.detect(f.readline())
    outlets = pd.read_csv(path, sep=';', encoding=result['encoding'])
    df = outlets.drop_duplicates(subset=['Торг_точка_грязная', 'Торг_точка_грязная_адрес'], keep='first')
    return df


def deduplicate(df, outlets_clean):
    dirty_outlets = df.copy()
    clean_outlets = outlets_clean.copy()
    index = 0

    for i in range(len(dirty_outlets)):
        outlet = dirty_outlets.iloc[i, 3]
        if outlet == '-':
            dirty_outlets.iloc[i, 4] = 'NULL'
        elif outlet == 'он же':
            check1 = re.sub('[,|.]', ' ', dirty_outlets.iloc[i-1, 2]).split() == re.sub('[,|.]', ' ', dirty_outlets.iloc[i, 2]).split()
            if check1:
                dirty_outlets.iloc[i] = dirty_outlets.iloc[i-1]
            else:
                dirty_outlets.iloc[i, 4] = index - 1
        else:
            element_before = dirty_outlets.iloc[i-1, 3]
            check1 = re.sub('[,|.]', ' ', dirty_outlets.iloc[i-1, 2]).split() == re.sub('[,|.]', ' ', dirty_outlets.iloc[i, 2]).split()
            check2 = re.sub('[,|.]', ' ', outlet).split() == re.sub('[,|.]', ' ', element_before).split()
            if check1 and check2:
                dirty_outlets.iloc[i] = dirty_outlets.iloc[i-1]
            else:
                clean_outlets.loc[len(clean_outlets)] = [len(clean_outlets), dirty_outlets.iloc[i, 3]]
                dirty_outlets.iloc[i, 4] = index
                index += 1             
    deduplicated = dirty_outlets.drop_duplicates(subset=['Торг_точка_грязная', 'outlet_clean_id'])
    clean_outlets
    return deduplicated, clean_outlets


def to_db(result, result_clean):
    engine = create_engine('sqlite:///task.db', echo=False)
    result.to_sql('outlets', con=engine)
    engine.execute("SELECT * FROM outlets").fetchall()
    result_clean.to_sql('outlets_clean', con=engine)
    engine.execute("SELECT * FROM outlets_clean").fetchall()


if __name__ == '__main__':
    df = read_csv_file('outlets.csv')
    df_clean = read_csv_file('outlets_clean.csv')
    result, result_clean = deduplicate(df, df_clean)
    to_db(result, result_clean)
