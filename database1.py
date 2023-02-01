import sqlite3
import pandas as pd

con = sqlite3.connect("carsharing.db", isolation_level=None)

cur = con.cursor()

cur.execute("""CREATE TABLE cars_table (id INTEGER PRIMARY KEY,
    timestamp TEXT,
    season TEXT,
    holiday TEXT,
    workingday TEXT,
    weather TEXT,
    temp REAL,
    temp_feel REAL,
    humidity REAL,
    windspeed REAL,
    demand INTEGER);""")

cars_table = pd.read_csv("CarSharing.csv")
cars_table.to_sql('cars_table', con, if_exists='append', index=False)

cur.execute("""
ALTER TABLE cars_table
ADD COLUMN temp_categories TEXT;
""")

cur.execute("""
UPDATE cars_table
SET temp_categories =
        CASE 
                WHEN temp_feel < 10 THEN 'Cold'
                WHEN temp_feel < 25 THEN 'Mild'
                WHEN temp_feel > 25 THEN 'Hot'
                ELSE 'Null' 
        END;
""")

cur.execute("""
        CREATE TABLE temperature
        AS SELECT temp, temp_feel, temp_categories FROM cars_table;
""")

cur.execute ("""
        ALTER TABLE cars_table
        DROP COLUMN temp;
""")

cur.execute("""
        ALTER TABLE cars_table
        DROP COLUMN temp_feel;
""")

res = cur.execute("""
        SELECT DISTINCT weather
        FROM cars_table;    
""")

for row in res:
        print(row[0])

cur.execute("""
        ALTER TABLE cars_table
        ADD COLUMN weather_code INTEGER;   
""")

cur.execute("""
        UPDATE cars_table
        SET weather_code =
                CASE
                        WHEN weather == 'Clear or partly cloudy' THEN 1
                        WHEN weather == 'Mist' THEN 2
                        WHEN weather == 'Light snow or rain' THEN 3
                        WHEN weather == 'heavy rain/ice pellets/snow + fog' THEN 4
                        ELSE 0
                END;
""")

cur.execute("""
        CREATE TABLE weather 
        AS SELECT weather, weather_code FROM cars_table;
""")

cur.execute("""
        ALTER TABLE cars_table
        DROP COLUMN weather;
""")

cur.execute("""
        CREATE TABLE time
        AS SELECT timestamp FROM cars_table;
""")

cur.execute("""
        ALTER TABLE time
        ADD COLUMN hour INTEGER;
""")

cur.execute("""
        ALTER TABLE time
        ADD COLUMN week_day TEXT;
""")

cur.execute("""
        ALTER TABLE time
        ADD COLUMN month_name TEXT;
""")

cur.execute("""
        UPDATE time
        SET hour = strftime('%H', timestamp);
""")

cur.execute("""
        UPDATE time
        SET week_day = strftime('%w', timestamp);        
""")

cur.execute("""
        UPDATE time
        SET month_name = strftime('%m', timestamp);
""")

# date and time with the highest demand in 2017
max_d = cur.execute("""SELECT MAX(demand), timestamp
            FROM cars_table
            WHERE timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59';
""")

for row in max_d:
    print(row)

# creating a table with the highest demand rate throughout 2017
cur.execute("""
        CREATE TABLE highest_demand2017 (
             week_name TEXT,
             month TEXT,
             season TEXT ,
             avg_demand REAL  
        );
""")
cur.execute("""
        INSERT INTO highest_demand2017
        SELECT
        CASE CAST (strftime('%w', date(timestamp)) AS integer)
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday'
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
                ELSE 'Saturday' end as week_name,
        CASE CAST (strftime('%m', date(timestamp)) as integer)
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
                ELSE 'December' end as month,
        season,
        AVG(demand) AS avg_demand
        FROM cars_table
        WHERE timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'
        GROUP BY week_name, month, season
        ORDER BY avg_demand DESC
        LIMIT 1;
""")

# creating a table with the lowest demand through out 2017
cur.execute("""CREATE TABLE lowest_demand2017(
        nameofweek TEXT,
        month TEXT,
        season TEXT,
        avg_demand REAL
        );
""")
cur.execute("""INSERT INTO lowest_demand2017
        SELECT
        CASE CAST(strftime('%w', date(timestamp)) as integer)
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday'
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
                ELSE 'Saturday' end as nameofweek,
        CASE CAST (strftime('%m', date(timestamp)) as integer)
                WHEN '01' THEN 'January'
                WHEN '02' THEN 'February'
                WHEN '03' THEN 'March'
                WHEN '04' THEN 'April'
                WHEN '05' THEN 'May'
                WHEN '06' THEN 'June'
                WHEN '07' THEN 'July'
                WHEN '08' THEN 'August'
                WHEN '09' THEN 'September'
                WHEN '10' THEN 'October'
                WHEN '11' THEN 'November'
                WHEN '12' THEN 'December'
                ELSE 'December' end as month,
        season,
        avg(demand) as avg_demand
        FROM cars_table
        WHERE timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'
        GROUP BY nameofweek, month, season
        ORDER BY avg_demand ASC
        LIMIT 1;
""")
# Fetching and printing the data.
h = cur.execute("""SELECT * FROM highest_demand2017;""")
for row in h:
        print(row)
        
l = cur.execute("""SELECT * FROM lowest_demand2017;""")
for row in l:
        print(row)
        
#7c The first query fetch the day of week with the highest demand rate throughout 2017.
a = cur.execute("""SELECT week_name FROM highest_demand2017;""")
for row in a:
        print(row)

# Then, create the table to show the average demand rate for different hours in the week.
highest_nameofweek = '0'
cur.execute("""
        CREATE TABLE hour_highest_demand AS
        SELECT strftime('%H', timestamp) AS hour,
        AVG(demand) AS avg_demand,
        CASE strftime('%w', timestamp)
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday'
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
                END AS weekday
        FROM cars_table
        WHERE strftime('%Y', timestamp) LIKE '2017%'
        AND strftime('%w', timestamp) = ?
        GROUP BY hour, weekday
        ORDER BY avg_demand DESC
""", highest_nameofweek,)

# Fetching and printing the data in the table
b = cur.execute("""SELECT * FROM hour_highest_demand;""")
for row in b:
        hour = row[0]
        avg_demand = row[1]
print(f"Hour: {hour}, Average Demand: {avg_demand}")

# Fetching the nameofweek of the lowest demand rate throughout 2017.
c = cur.execute("""SELECT nameofweek FROM lowest_demand2017;""")
for row in c:
        print(row)
# Then, create a table to show the average demand rate for different hours of the week.
lowest_weekday = '1'
cur.execute("""
        CREATE TABLE hour_lowest_demand AS
        SELECT strftime('%H', timestamp) AS hour,
        AVG(demand) AS avg_demand,
        CASE strftime('%w', timestamp)
                WHEN '0' THEN 'Sunday'
                WHEN '1' THEN 'Monday'
                WHEN '2' THEN 'Tuesday'
                WHEN '3' THEN 'Wednesday'
                WHEN '4' THEN 'Thursday'
                WHEN '5' THEN 'Friday'
                WHEN '6' THEN 'Saturday'
                END AS weekday
        FROM cars_table
        WHERE strftime('%Y', timestamp) LIKE '2017%'
        AND strftime('%w', timestamp) = ?
        GROUP BY hour, weekday
        ORDER BY avg_demand DESC
""", lowest_weekday,)

# Fetch and print the data in the table
d = cur.execute("""SELECT * FROM hour_lowest_demand;""")
for row in d:
        hour = row[0]
        avg_demand = row[1]
        print(f"Hour: {hour}, Average Demand: {avg_demand}")

 
# most prevalent weather in 2017
weather_count = cur.execute("""
        SELECT temp_categories, COUNT(temp_categories) AS count
        FROM cars_table
        WHERE timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'
        GROUP BY temp_categories
        ORDER BY count DESC;
""")

for row in weather_count:
    print(row)
print("The most prevalent weather in 2017 was Mild")

# creating a table to show the highest, lowest and average windspeed for each month in 2017
cur.execute("""
        CREATE TABLE windspeed
        AS SELECT (strftime('%m', timestamp)) as month, MAX(windspeed) AS max_windspeed, 
        MIN(windspeed) AS min_windspeed, AVG(windspeed) AS average_windpeed
        FROM cars_table
        WHERE timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'
        GROUP BY (strftime('%m', timestamp));
""")

# creating a table to show the highest, lowest and average humidity for each month in 2017
cur.execute("""
        CREATE TABLE humidity
        AS SELECT (strftime('%m', timestamp)) as month, MAX(humidity) AS max_humidity, 
        MIN(humidity) AS min_humidity, AVG(humidity) AS average_humidity
        FROM cars_table
        WHERE timestamp BETWEEN '2017-01-01 00:00:00' AND '2017-12-31 23:59:59'
        GROUP BY (strftime('%m', timestamp));
""")

# creating a table to show average demand for the weather types in 2017
cur.execute("""
        CREATE TABLE average_demand
        AS SELECT temp_categories, AVG(demand) AS average
        FROM cars_table
        GROUP BY temp_categories
        ORDER BY average DESC;
""")

con.close()


