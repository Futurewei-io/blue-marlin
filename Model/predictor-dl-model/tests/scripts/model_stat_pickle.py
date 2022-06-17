from pyspark import SparkContext
from pyspark.sql import HiveContext
import pickle

if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    hive_context = HiveContext(sc)

    TABLE_NAME = 'dlpm_05172022_model_stat'

    command = """SELECT * FROM {}""".format(TABLE_NAME)
    model = hive_context.sql(command).collect()[0]
    model = model.asDict()
    model['model_info'] = model['model_info'].asDict()
    model['stats'] = model['stats'].asDict()

    print(model)

    with open(TABLE_NAME+'.pickle', 'wb') as handle:
        pickle.dump(model, handle, protocol=pickle.HIGHEST_PROTOCOL)


    

