import pandas as pd
import numpy as np

from os.path import (
    join,
    abspath,
    dirname,
)
from os import getcwd
from itertools import groupby
from bson import ObjectId
from datetime import (
    datetime,
    timedelta,
)
from json import (
    JSONEncoder
)
from pandas import (
    read_csv,
    to_numeric,
)

def get_date_range(num_days):
    end_date = datetime.now()-timedelta(days=1)
    end_date = end_date.replace(
        hour=11,
        minute=59,
        second=59,
    )
    start_date = datetime.now()-timedelta(days=num_days)
    start_date = start_date.replace(
        hour=0,
        minute=0,
        second=0,
    )
    return start_date, end_date

def get_str_from_time(date):
    return date.strftime('%Y-%m-%d %H:%M:%S')

def clean(df, service):
    files = {
        'rides': {
            'user': 'QA_rides_provider.csv',
        },
        'food': {
            'provider': 'QA_food_providers.csv',
            'user': 'QA_food_users.csv',
        },
    }
    pwd = dirname(abspath(__file__))
    for user_type in files[service]:
        col = user_type+'_id'
        if col in df.columns.tolist():
            df[col] = to_numeric(
                df[col],
                errors='coerce',
            )
            df_qa = read_csv(
                join(
                    pwd,
                    files[service][user_type],
                ),
            )
            df = df[~df[col].isin(df_qa['id'].to_numpy())]
            df = df[df[col] > 0]
    df = df.reset_index(drop=True)
    return df

def is_float(x):
    if isinstance(x, np.float):
        return True
    if isinstance(x, float):
        return True
    return False

def JSONEncoderCustom(JSONEncoder):
    def default(self, o):
        if o != o:
            return ''
        if isinstance(o, ObjectId):
            return str(o)
        if isinstance(o, datetime):
            return get_str_from_time(o)
        return JSONEncoder.default(self, o)

def get_str(x):
    res = ''
    if isinstance(x, list):
        res = ''
        for i, a in enumerate(x):
            if a != a:
                continue
            if i > 0:
                res += ','
            if isinstance(a, datetime):
                res += get_str_from_time(a)
            elif is_float(a):
                if a-int(a) == 0.0:
                    res += str(int(a))
                    #print(a, str(int(a)))
                else:
                    #print(a, str(a))
                    res += str(a)
            else:
                res += str(a)
        return res
    elif isinstance(x, dict):
        json_encoder = JSONEncoderCustom()
        return json_encoder.encode(x)
    elif isinstance(x,ObjectId):
        return str(x)
    else:
        if x != x:
            if isinstance(x, float):
                return None
            return ''
        else:
            if isinstance(x, datetime):
                return get_str_from_time(x)
            if is_float(x):
                if x-int(x) == 0.0:
                    return str(int(x))
            return str(x)
    return res

def add_quote(text):
    return '"'+text+'"'

def date_to_str(date):
    return '"'+date.strftime('%Y-%m-%d %H:%M:%S')+'"'

def write_to_db(db, table, df, ignore_duplicates=False, replace_duplicates=False):
    """
    Pass ignore_duplicates=True to prevent duplicate entries in DB.
    Pass replace_duplicates=True to update duplicate entries in DB.
    Note: db must have a primary key for this to work.
    """
    db.insert_many(
        fields=df.columns.tolist(),
        values=df.to_numpy(),
        table_name=table,
        ignore_duplicates=ignore_duplicates,
        replace_duplicates=replace_duplicates
    )

def do_groupby(df, group_by, group_on):
    df_group = df.groupby(
        group_by,
        as_index=False,
    )[group_on].agg(lambda x: list(x))
    
    return df_group

def get_reasons(df_booking, threshold=3, col_name='booking_id', is_panel=0):
    if col_name not in df_booking.columns:
        raise ValueError(col_name+' not in columns')
    df_booking['count'] = df_booking[col_name].apply(
        lambda x: len(x)
    )
    reason = 'Threshold: '+str(threshold)+', Count: '
    df_booking['reason'] = df_booking['count'].apply(
        lambda x: reason+str(x)
    )
    if is_panel == 1:
        df_booking['res_has_panel'] = df_booking['res_has_panel'].apply(
            lambda x: 'yes' if x == 1 else 'no'
        )
        df_booking['reason'] = df_booking['reason'].apply(
            lambda x: x+', Panel: '
        )
        df_booking['reason'] = df_booking['reason']+df_booking['res_has_panel']

def get_flags(df, threshold):
    if 'count' not in df.columns:
        raise ValueError('`count` not in columns')
    df = df[df['count'] >= threshold]
    return df

def get_graph_from_groupby(df, group_by, group_on, threshold):
    df_group = do_groupby(df, group_by, group_on)
    print(df_group.head())
    rows = df_group.to_numpy()
    weights = {}
    for row in rows[:]:
        weight_cur = {}
        # print(row[0])
        for key, group in groupby(row[1]):
            frequency = len(list(group))
            # print(key, frequency)
            if frequency < threshold:
                continue
            weight_cur[key] = frequency
        if len(weight_cur) < 1:
            continue
        weights[row[0]] = weight_cur
    return weights

def get_dict_value(d, key1, key2=None, key3=None):
    """Return d[key1][key2][key3] if valid, return NaN otherwise."""
    try:
        if key2 is None:
            return d[key1]
        if key3 is None:
            return d[key1][key2]
        return d[key1][key2][key3]
    except (TypeError, ValueError, KeyError):
        return float("nan")
    except Exception as e:
        print(e)
        raise
