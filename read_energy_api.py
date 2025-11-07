import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import requests

# also need pyarrow



API_STRING = 'https://api.eia.gov/v2/electricity/rto/region-data/data/?frequency=hourly&data[0]=value&facets[respondent][]=TVA&facets[type][]=D&start={0}&end={1}&sort[0][column]=period&sort[0][direction]=asc&offset=0&length=5000&api_key={2}'



def get_api_key(api_key_filepath: str) -> str:
    with open(api_key_filepath, 'r') as f:
        api_key = f.read()

    api_key = api_key.strip()
    return api_key


def split_timeframe(first_date: str, last_date: str) -> list:
    first_date = pd.to_datetime(first_date)
    last_date = pd.to_datetime(last_date)

    # go from midnight on the first day to 11:00pm on the last day
    if last_date.hour != 23:
        last_date = last_date.replace(hour=23)

    # get list of start/end dates
    date_list = list(np.arange(first_date, last_date, timedelta(hours=4900)))
    if date_list[-1] != last_date:
        date_list.append(last_date)

    print(date_list)

    return date_list


def call_api(start_date: np.datetime64, end_date: np.datetime64 | pd.Timestamp, api_key: str) -> pd.DataFrame:
    start_date = start_date.astype(datetime)
    if type(end_date)==np.datetime64:
        end_date = end_date.astype(datetime)
    formatted_start = datetime.strftime(start_date, '%Y-%m-%dT%H')
    formatted_end = datetime.strftime(end_date, '%Y-%m-%dT%H')

    response = requests.get(API_STRING.format(formatted_start, formatted_end, api_key))
    json_data = response.json()
    df = pd.DataFrame.from_dict(json_data['response']['data'])

    return df


def paginate_results(start_date: str, end_date: str, api_file: str):
    api_key = get_api_key(api_file)
    time_list = split_timeframe(start_date, end_date)
    df = None

    for i in range(len(time_list)-1):
        temp_start = time_list[i]
        temp_end = time_list[i+1]
        print(f'Reading dataset from {temp_start} to {temp_end}')

        temp_df = call_api(temp_start, temp_end, api_key)
        if df is None:
            df = temp_df
        else:
            df = pd.concat([df, temp_df])

    return df


def clean_dataframe(df: pd.DataFrame):
    # only keep necessary columns
    df = df[['period', 'value']]

    # change to datetime
    df['period'] = pd.to_datetime(df['period'], format='%Y-%m-%dT%H')


def main():
    start = '2023-01-01'
    end = '2024-12-31'
    api_path = 'secret_api.txt'

    df = paginate_results(start, end, api_path)
    # clean_dataframe(df)

    # save as parquet - don't have to fix dates again
    df.to_parquet('tva_load.parquet')



if __name__ == '__main__':
    main()