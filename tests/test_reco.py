import pytest
from recoWheel.reco import reconciliationDF
from recoWheel.spark import get_spark

nested_JSON = [{
  "field1": 1, 
  "field2": 2,
  "field3": 3, 
  "field4": [1,2,3,4],
  "nested_array":[{
     "nested_field1": 3,
     "nested_field2": 4
  },
  {
     "nested_field1": 5,
     "nested_field2": 6
  }]
}]
source_df = get_spark().createDataFrame(nested_JSON)

actual_df = reconciliationDF(source_df)

expected_data = [{"field1": 961816747, "field2": 898633904, "field3": 275678139, "field4": 2676236688, "nested_array": 2438280561}]

expected_df = get_spark().createDataFrame(expected_data)

assert(expected_df.collect() == actual_df.collect())