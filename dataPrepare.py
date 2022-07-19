import pandas as pd
import calendar
from shapely.geometry import Point

df = pd.read_csv('data.csv')
df['Date/Time'] = pd.to_datetime(df['Date/Time'])
df['Date/TimeFloor'] = df['Date/Time'].dt.floor('1H')

df['Date'] = df['Date/Time'].dt.date
df['Hour'] = df['Date/Time'].dt.hour

df['WeekdayNumber'] = df['Date/Time'].dt.weekday
df["WeekdayName"] = df["WeekdayNumber"].apply(lambda x: calendar.day_name[x])

df['WeekmonthNumber'] = df['Date/Time'].dt.month
df['WeekmonthName'] = df['WeekmonthNumber'].apply(lambda x: calendar.month_name[x])
df['Geo'] = df.apply(lambda row:Point(row['Lat'],row['Lon']), axis=1)

df.to_csv('myData.csv', index=False)






