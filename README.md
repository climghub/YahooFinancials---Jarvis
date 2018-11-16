# YahooFinancials---Jarvis
My personal stock market analyzer built on yahoofinancials, written on Python 3

1. To begin, refer to [GitHub repository](https://github.com/JECSand/yahoofinancials) on how to use yahoofinancials in Python

2. The following modules are needed to be installed if it's not in your Python libraries
- pandas
- json
- sqlite3
- numpy

3. Stock names and tickers are stored in [stocklistfile](stocklistfile.py), make sure you store this py in the same folder as [main code](stockJarvis.py)

4. To run, first select the exchange
```
exchange = 'SG'
```

5. Then on first run, set first_time to True to first create the database
```
first_time = True
```

6. Then get archive data by setting the following:
```
read_data = False
get_archive = True
```
and also select the dates needed, then re-run the programme
```
start_date = '2000-01-01'
end_date = '2018-12-31'
```
7. If you wish to see what my Jarvis does, toggle the mode again and re-run the programme
```
read_data = True
```
