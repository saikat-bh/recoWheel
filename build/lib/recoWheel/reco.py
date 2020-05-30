from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, LongType, ArrayType, IntegerType
from recoWheel.spark import get_spark
from functools import reduce
from operator import add

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = prefix + '.' + field.name if prefix else field.name
        dtype = field.dataType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(name)

    return fields


def explodeDF(df):
  for (name, dtype) in df.dtypes:
    if "array" in dtype:
      df = df.withColumn(name, F.explode(name))
  return df

def df_is_flat(df):
    for (_, dtype) in df.dtypes:
        if ("array" in dtype) or ("struct" in dtype):
            return False

    return True

def flatJson(jdf):
  keepGoing = True
  while(keepGoing):
    fields = flatten(jdf.schema)
    new_fields = [item.replace(".", "_") for item in fields]
    jdf = jdf.select(fields).toDF(*new_fields)
    jdf = explodeDF(jdf)
    if df_is_flat(jdf):
        keepGoing = False
  
  columnList = jdf.columns
  if len(columnList) > 1:     
    nestedDF = jdf.select([F.sum(F.conv(F.sha1(F.col(x).cast(StringType())).substr(-8,8),16,10).cast(LongType())) for x in columnList])
    nestedSumDF = nestedDF.select((reduce(add, (F.col(x) for x in nestedDF.columns)))).collect()[0][0]
    return nestedSumDF
  else:
    flatDF = jdf.select([F.sum(F.conv(F.sha1(F.col(columnList[0]).cast(StringType())).substr(-8,8),16,10).cast(LongType()))]).collect()[0][0]
    return flatDF

def reconciliationDF(reconDF):
  hashDict = {}
  for item in reconDF.columns:
    hashDict[item] = flatJson(reconDF.select(item))

  hashDF = get_spark().createDataFrame([hashDict])  
  return hashDF