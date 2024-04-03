import pandas as pd
import numpy as np
from datetime import datetime
import pytz

def split_interval_to_list(start, end, n_parts, format_string: str = '%H:%M'):
    """split interval into N parts, giving a tuple of times
    Args:
        start_string (str): start time as string
        end_string (str): end time as string
        n_parts (int, optional): numper of pieces. Defaults to 2.
        format_string (str, optional): format string. Defaults to '%H:%M'.

    Returns:
        list: list of times, including beginning and end
    """

    delta = end - start
    if n_parts is None or n_parts == 1:
        return (start.strftime(format_string), end.strftime(format_string))

    result = [(start + (delta/n_parts) * i)
              for i in range(n_parts)]
    result.append(end)

    return result


def create_coords(start, end, coord_string):
    """create coordinates from string of coordinates and equal time intervals from time range (start,end).
    Args:
        start (datetime): start time
        end (datetime): end time
        coord_string (str): string of coordinates

    Returns:
        tuple: tuple of list of longitudes, latitude, start and end times including beginning and end
    """

    bw_coords = coord_string.split(',')[:-1]  # split string and remove the last '0' present in string
    bw_coords = [coord.split()[-1] for coord in bw_coords]

    new_coords_long = [round(float(bw_coords[i]), 6) for i in range(0, len(bw_coords), 2)]
    new_coords_lat = [round(float(bw_coords[i]), 6)for i in range(1, len(bw_coords), 2)]

    format_string = '%Y-%m-%d%H:%M:%S'
    result = split_interval_to_list(start, end, len(new_coords_lat), format_string)

    new_coords_start_time = [result[i] for i in range(0, len(result) - 1)]
    new_coords_end_time = [result[i] for i in range(1, len(result))]

    return (new_coords_long, new_coords_lat, new_coords_start_time, new_coords_end_time)


def main():
    df = pd.read_csv('../history-2024-03-07.csv')
    df.drop(['name', 'address', 'ExtendedData/Data/0/value', 'ExtendedData/Data/0/_name', 'ExtendedData/Data/1/_name',
             'ExtendedData/Data/2/_name', 'description', 'altitude', 'LineString/altitudeMode', 'LineString/extrude',
             'LineString/tesselate'
             ], axis=1, inplace=True)

    df.columns = ['Activity', 'Distance', 'TimeSpan/begin', 'TimeSpan/end', 'longitude', 'latitude',
                  'LineString/coordinates']

    df['TimeSpan/begin'][0] = df['TimeSpan/begin'][0][:11] + '14:30:00' + df['TimeSpan/begin'][0][19:]

    df['TimeSpan/begin'] = pd.to_datetime(df['TimeSpan/begin'])
    df['TimeSpan/end'] = pd.to_datetime(df['TimeSpan/end'])

    est = pytz.timezone('US/Eastern')
    df['TimeSpan/begin'] = df['TimeSpan/begin'].apply(lambda x: x.astimezone(est))
    df['TimeSpan/end'] = df['TimeSpan/end'].apply(lambda x: x.astimezone(est))


    new_coords_long = []
    new_coords_lat = []
    new_coords_start_time = []
    new_coords_end_time = []
    new_coords_activity = []

    for index, row in df.iterrows():
        if pd.isnull(row['longitude']):
            result = create_coords(row['TimeSpan/begin'], row['TimeSpan/end'], row['LineString/coordinates'])
            new_coords_long.extend(result[0])
            new_coords_lat.extend(result[1])
            new_coords_start_time.extend(result[2])
            new_coords_end_time.extend(result[3])
            tmp = [row['Activity']] * len(result[0])
            new_coords_activity.extend(tmp)

    new_coords_df = pd.DataFrame({'Activity': new_coords_activity,
                                  'longitude': new_coords_long, 'latitude': new_coords_lat,
                                  'TimeSpan/begin': new_coords_start_time, 'TimeSpan/end': new_coords_end_time}
                                 )
    new_coords_df['LineString/coordinates'] = np.nan
    new_coords_df['Distance'] = np.nan

    df = pd.concat([df[df['longitude'].notna()], new_coords_df]).sort_values(['TimeSpan/begin']).drop(
        ['Distance', 'LineString/coordinates'], axis=1).reset_index(drop=True)

    df['TimeSpan/begin'] = df['TimeSpan/begin'].dt.round('1s')
    df['TimeSpan/end'] = df['TimeSpan/end'].dt.round('1s')

    df.to_csv('../final_df.csv', index=False)


if __name__ == "__main__":
    main()

