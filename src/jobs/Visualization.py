import pycountry
import pycountry_convert as pcc
from fuzzywuzzy import process
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import plotly.express as pe
spark = SparkSession.builder.appName('Data Transformations').getOrCreate()

df = spark.read.csv('input/visa_number_in_japan.csv', header=True, inferSchema=True)

# standardize
new_column_names = [col_name.replace(' ', '_')
                    .replace('/', ' ')
                    .replace('.', ' ')
                    .replace(',', ' ')
                    for col_name in df.columns]

df = df.toDF(*new_column_names)

df = df.dropna(how='all')

df = df.select('year', 'country', 'number_of_issued_numerical')


def correct_country_names(name, threshold=85):
    countries = [country.name for country in pycountry.countries]

    corrected_name, score = process.extractOne(name, countries)

    if score >= threshold:
        return corrected_name
    # no changes on name

    return name


def continent_name(country_name):
    try:
        country_code = pcc.country_name_to_country_alpha2(country_name, cn_name_format='default')
        continent_code = pcc.country_alpha2_to_continent_code(country_code)
        return pcc.convert_continent_code_to_continent_name(continent_code)
    except:
        return None


correct_country_name_udf = udf(correct_country_names, StringType())

df = df.withColumn('country', correct_country_name_udf(df['country']))

correct_country = {
    'Andra': 'Russia',
    'Antigua Berrouda': 'Antigua and Barbuda',
    'Barrane': 'Bahrain',
    'Brush': 'Bhutan',
    'Komoro': 'Comoros',
    'Benan': 'Benin',
    'Kiribass': 'Kiribati',
    'Gaiana': 'Guyana',
    'Court Jiboire': "Côte d'Ivoire",
    'Lesot': 'Lesotho',
    'Macau travel certificate': 'Macao',
    'Moldoba': 'Moldova',
    'Naure': 'Nauru',
    'Nigail': 'Niger',
    'Palao': 'Palau',
    'St.Christopher Navis': 'Saint Kitts and Nevis',
    'Santa Principa': 'Sao Tome and Principe',
    'Saechel': 'Seychelles',
    'Slinum': 'Saint Helena',
    'Swaji Land': 'Eswatini',
    'Torque menistan': 'Turkmenistan',
    'Tsubaru': 'Zimbabwe',
    'Kosovo': 'Kosovo'

}

df = df.replace(correct_country, subset='country')

udf_continent = udf(continent_name, StringType())

df = df.withColumn('continent', udf_continent(df['country']))
#view
df.createGlobalTempView('japan_visa')

df_continent =spark.sql("""
       SELECT year, continent, sum(number_of_issued_numerical) visa_issued
       FROM global_temp.japan_visa
       WHERE continent is NOT NULL
       GROUP BY year, continent
""")

df_continent = df_continent.toPandas()

fig = pe.bar(df_continent, x='year', y='visa_issued', color='continent', barmode = "group")

fig.update_layout(title_text ="visa issued in japan by their continent",
                  xaxis_title='Year',
                  yaxis_title='Number of visa issued',
                  legend_title='Continent')

fig.write_html('output/visa_number_in_japan_country.html')

#top 14 countries with most issued visa document
df_country =spark.sql("""
       SELECT country, sum(number_of_issued_numerical) visa_issued
       FROM global_temp.japan_visa
       WHERE country NOT IN ('total','others')
       AND country IS NOT NULL
       AND year = 2017
       GROUP BY country
       order by visa_issued DESC
       LIMIT 14
""")

df_country = df_country.toPandas()

fig = pe.bar(df_country, x='country', y='visa_issued', color='country')

fig.update_layout(title_text="14 most visa issued in some countries",
                  xaxis_title='Country',
                  yaxis_title='Number of visa issued',
                  legend_title='Country')

fig.write_html('output/visa_number_in_japan_by_the_country.html')

df_country_map = spark.sql("""
        SELECT year, country,sum(number_of_issued_numerical) visa_issued
        FROM global_temp.japan_visa
        WHERE country not in ('total','years')
        and country is not null
        group by year, country
        ORDER BY year asc
""")

df_country_map = df_country_map.toPandas()

fig = pe.choropleth(df_country_map, locations='country',
                    color='visa_issued',
                    hover_name='country',
                    animation_frame='year',
                    range_color=[100000,100000],
                    color_continuous_scale=pe.colors.sequential.Plasma,
                    locationmode='country names',
                    title='Visa issued by countries on those years')

fig.write_html('output/visa_number_in_japan_by_year_map.html')

df.write.csv("output/visa_number_in_japan_refreshed.csv", header=True, mode='overwrite')

spark.stop()
