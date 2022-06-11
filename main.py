#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count

spark = SparkSession.builder.master("local[1]") \
    .appName('SparkByExamples.com') \
    .getOrCreate()

#%%
# Data Paths 
path_list = "/Users/xaldigital/Documents/EjerciciosPracticaXD/PySpark/encontrando_aeropuerto/Data/airportList.csv"
path_location = "/Users/xaldigital/Documents/EjerciciosPracticaXD/PySpark/encontrando_aeropuerto/Data/airportsLocation.csv"

# Reading CVS files 
df_alist = spark.read.option("header", True).option("inferSchema", True).csv(path_list)
df_location = spark.read.option("header", True).option("inferSchema", True).csv(path_location)

# %%
# showing df airport list
df_alist.printSchema()
df_alist.show()

#%%
# showing df airport location
df_location.printSchema()
df_location.show()


# %%
# Removing null values from list airports df in airport_code column 
df_alist_filtered = df_alist.filter(df_alist.airport_code.isNotNull())
df_alist_filtered.count()
df_alist_filtered.show()

# %%
#Removing null values from location airpots df in airport_code column
df_location_filtered = df_location.filter(df_location.airport_code.isNotNull())
df_location_filtered.count()


# %%
df_join = df_alist_filtered.join(df_location_filtered, ["airport_code"], "left")
df_join.count()
# confirmed_airports = df_alist_filtered.filter(df_alist_filtered.)

# %%
# Null and NaN Values count
# df_join.select([count(when(isnan(c), c)).alias(c) for c in df_join.columns]).show()

df_join.select([count(when(col(c).contains('None') | col(c).contains('NULL') | \
                            (col(c) == '' ) | col(c).isNull() | isnan(c), c)).alias(c)
                    for c in df_join.columns]).show()

# %%
# Confirmed Airports
confirmed_airports = df_join.filter(df_join.latitude >= 40)
print("Aeropuertos confirmados con trafico de armas: {} \n".format(confirmed_airports.count()))
confirmed_airports.show()


# %%
# Targeted countries list 
targeted_count = df_join.filter((df_join.country == "United States") | (df_join.country == "Mexico") \
    | (df_join.country == "Brazil") | (df_join.country == "Canada") | (df_join.country == "Japan"))

print("lista de paises objetivo: {}".format(targeted_count.count()))
targeted_count.show()


# %%
# list of countruies with odd id  
suspect_countries = df_join.filter((df_join.airport_id%2) > 0)
print("Lista de paises con id impar: {}".format(suspect_countries.count()))
suspect_countries.show()


# %%
# All in one filter
entire_list = df_join.filter(((df_join.airport_id%2) > 0) \
    | (df_join.country == "United States") | (df_join.country == "Mexico") \
    | (df_join.country == "Brazil") | (df_join.country == "Canada") | (df_join.country == "Japan"))

print("Aeropuertos en riesgo: {}".format(entire_list.count()))
entire_list.show()
