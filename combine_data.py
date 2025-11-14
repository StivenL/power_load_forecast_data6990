import pandas as pd
import numpy as np
import os
import holidays


def read_power(filepath: str) -> pd.DataFrame:
    print('Reading power file')
    df = pd.read_parquet(filepath)

    # rename columns to match weather
    df.rename(columns={'period': 'date', 'value': 'power'}, inplace=True)

    # add time zone
    df['date'] = df['date'].dt.tz_localize('UTC')

    return df


def read_weather(filepath: str) -> pd.DataFrame:
    print('Reading weather from', filepath)
    df = pd.read_parquet(filepath)

    # change temperature from C to F
    df['temperature_2m'] = (df['temperature_2m'] * 9/5) + 32
    df['apparent_temperature'] = (df['apparent_temperature'] * 9/5) + 32

    # change precipitation from mm/cm/m to inches
    df['precipitation'] = df['precipitation'] / 25.4  # mm
    df['rain'] = df['rain'] / 25.4  # mm
    df['snowfall'] = df['snowfall'] / 2.54  # cm
    df['snow_depth'] = df['snow_depth'] / 0.0254  # m

    # change wind speed from km/h to mph
    df['wind_speed_10m'] = df['wind_speed_10m'] / 1.609

    # change is_day to bool
    df['is_day'] = df['is_day'] == 1

    # change weather code to general condition as dummy columns
    # range from 0 to 99, don't want to add potential 300 features to the final dataset
    # https://www.nodc.noaa.gov/archive/arc0021/0002199/1.1/data/0-data/HTML/WMO-CODE/WMO4677.HTM
    df['weather_code'] = np.select(
        [
            df['weather_code']<=49, df['weather_code']<=59, df['weather_code']<=69,
            df['weather_code']<=79, df['weather_code']<=99
        ],
        ['none', 'drizzle', 'rain', 'solid precipitation', 'thunderstorm'],
        default='Unknown')
    df = pd.get_dummies(df)

    # add city name to all columns except date
    prefix = os.path.basename(filepath).split('-', 1)[0] + '_'
    df = df.add_prefix(prefix)
    df.rename(columns={prefix+'date': 'date'}, inplace=True)

    return df


def read_files(data_folder: str = 'data') -> pd.DataFrame:
    file_list = os.listdir(data_folder)
    weather_df = None

    for f in file_list:
        if f == 'tva_load.parquet':
            power_df = read_power(os.path.join(data_folder, f))
        elif f.endswith('hourly-data.parquet'):
            tmp_df = read_weather(os.path.join(data_folder, f))
            if weather_df is None:
                weather_df = tmp_df
            else:
                weather_df = pd.merge(weather_df, tmp_df, on='date', how='outer')

    # combine dataframes
    df = pd.merge(power_df, weather_df, on='date', how='inner')

    return df


def clean_dataframe(old_df: pd.DataFrame) -> pd.DataFrame:
    print('Cleaning dataframe')
    df = old_df.copy()

    # remove impossible negative power (2 rows)
    df = df[df['power'] > 0]

    # remove crazy outlier power (2 rows)
    df = df[df['power'] < 900000]

    # remove weather_code_none - unnecessary given other dummies columns
    df = df.drop(columns=['knoxville_weather_code_none',
                          'memphis_weather_code_none',
                          'nashville_weather_code_none'])

    return df


def add_dates(old_df: pd.DataFrame) -> pd.DataFrame:
    print('Adding dates')
    # get list of holidays
    usa_holidays = holidays.US(years=[2023,2024])

    df = old_df.copy()

    # get date information
    df['holiday'] = df['date'].dt.date.isin(usa_holidays)
    df['is_weekend'] = df['date'].dt.weekday >= 5  # 5 is Saturday, 6 is Sunday
    df['year'] = df['date'].dt.year
    df['month'] = df['date'].dt.month
    df['day_percent'] = df['date'].dt.day / df['date'].dt.daysinmonth
    df['hour'] = df['date'].dt.hour

    # get season info
    df['season'] = np.select(
        [df['month']<=2, df['month']<=5, df['month']<=8, df['month']<=11, df['month']==12],
        ['Winter', 'Spring', 'Summer', 'Fall', 'Winter'],
        default='Unknown')
    df = pd.get_dummies(df)

    # get hours since start
    start_date = df['date'].min()
    df['runtime'] = (df['date'] - start_date).dt.total_seconds() / 3600

    # get lagged power
    df.sort_values(by='date', ascending=True, inplace=True)
    df['power_1hr'] = df['power'].shift(1)
    df['power_1day'] = df['power'].shift(24)

    # remove rows with missing values from lag
    df = df.dropna(subset=['power_1day'])

    return df


if __name__ == '__main__':
    print('Running file')
    original_df = read_files()
    cleaned_df = clean_dataframe(original_df)
    cleaned_df = add_dates(cleaned_df)

    # final dataset - 17490 rows, 57 columns
    cleaned_df.to_parquet('data/combined_files.parquet')
