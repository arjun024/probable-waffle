import pdb
import psycopg2
import pandas.io.sql as sqlio
# from config import config

conn = psycopg2.connect("dbname=adult user=ben password=postgres")

sql = "select age, race, avg(capital_gain), sum(capital_gain) from adult group by grouping sets ((age), (race));"
df = sqlio.read_sql_query(sql, conn)
pdb.set_trace()

