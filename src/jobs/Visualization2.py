import pycountry
import pycountry_convert as pcc
from fuzzywuzzy import process
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import plotly.express as pe

spark = SparkSession.builder.appName('Data Transforms').getOrCreate()

df = spark.read.csv('input/Hospital_Discharge.csv', header=True, inferSchema=True)

# standardize
new_column_names = [col_name.replace(' ', '_')
                    .replace('/', ' ')
                    .replace('.', ' ')
                    .replace(',', ' ')
                    for col_name in df.columns]

df = df.toDF(*new_column_names)

df = df.dropna(how='all')

df = df.select('Provider_State', 'Provider City', 'Average Total Payments')





correct_state = {
'al': 'albama',
'ak' : 'alaska',
'az' : 'arizona',
'ar' : 'arkansas',
'ca': 'california',
'co' : 'colorado',
'ct' : 'connectitcut',
'de' : 'delaware',
'dc' : 'washington, D.C.,',
'fl' : 'florida',
'ga' : 'georgia',
'hi' : 'hawaii',
'id' : 'idaho',
'il' : 'illinois',
'in' : 'indiana',
'la' : 'lowa',
'ks' : 'kansas',
'ky' : 'kentucky',
'la' : 'louisiana',
'me' : 'maine',
'md' : 'maryland',
'ma' : 'massachusetts',
'mi' : 'michigan',
'mo' : 'missouri',
'mn' : 'minnesota',
'ms' : 'mississippi',
'tx' : 'texas',
'mt' : 'montana',
'ne' : 'nebraska',
'nv' : 'nevada',
'nh' : 'new hampshire',
'nj' : 'new jersey',
'nm' : 'new mexico',
'ny' : 'new york state',
'nc' : 'north california',
'nd' : 'north dakota',
'oh' : 'ohio',
'ok' : 'oklahoma',
'or' : 'oregon',
'pa' : 'pennsylvania',
'ri' : 'rhode island',
'sc' : 'south california',
'sd' : 'south dakota',
'tn' : 'tennessee',
'ut' : 'utah',
'vt' : 'vermot',
'va' : 'virginia',
'wa' : 'washington',
'wv' : 'west virginia',
'wi' : 'wisconsin',
'wy' : 'wyoming'


}

df = df.replace(correct_state, subset='Provider_State')


df.createGlobalTempView('Hospital_Charges')

df_continent =spark.sql("""
       SELECT Provider_State, Provider City, sum(Average Total Payments) visa_issued
       FROM global_temp.Hospital_Charges
       WHERE continent is NOT NULL
       GROUP BY year, continent
""")

df_continent = df_continent.toPandas()

fig = pe.bar(df_continent, x='year', y='visa_issued', color='continent', barmode = "group")

fig.update_layout(title_text ="The average discharge total amount",
                  xaxis_title='Provider_State',
                  yaxis_title='Average Total Payments',
                  legend_title='Total Discharge')

fig.write_html('output/the_avergae_discharge_total_amount.html')

df.write.csv("output/the_avergae_discharge_total_amount.csv", header=True, mode='overwrite')

spark.show()

spark.stop()



#al - albama
#ak - alaska
#az - arizona
#ar - arkansas
#ca - california
#co - colorado
#ct - connectitcut
#de - delaware
#dc - washington, D.C.,
#fl - florida
#ga - georgia
#hi - hawaii
#id - idaho
#il - illinois
#in - indiana
#la - lowa
#ks - kansas
#ky - kentucky
#la - louisiana
#me - maine
#md - maryland
#ma - massachusetts
#mi - michigan
#mo - missouri
#mn - minnesota
#ms - mississippi
#tx - texas
#mt - montana
#ne - nebraska
#nv - nevada
#nh - new hampshire
#nj - new jersey
#nm - new mexico
#ny - new york state
#nc - north california
#nd - north dakota
#oh - ohio
#ok - oklahoma
#or - oregon
#pa - pennsylvania
#ri - rhode island
#sc - south california
#sd - south dakota
#tn - tennessee
#ut - utah
#vt - vermot
#va - virginia
#wa - washington
#wv - west virginia
#wi - wisconsin
#wy - wyoming




#Hospital Discharges data
#yet to work