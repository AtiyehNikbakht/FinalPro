import pandas as pd

df = pd.read_csv('myData.csv')

df['Date/Time'] = pd.to_datetime(df['Date/Time'])
df['WeekNumber'] = df['Date/Time'].dt.strftime('%U')
df['Time'] = df['Date/Time'].dt.floor('12H')
df['Time12'] = df['Time'].dt.time

df.to_csv('cassandraData.csv', index=False)





