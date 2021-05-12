#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0.html

#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

from pyspark import SparkContext
from pyspark.sql import HiveContext


def write_uckey_csv(df, filepath):
    with open(filepath, 'w') as f:
        count = 0
        uckeys = set()
        f.write('uckey,slot_type,slot_id,network,gender,age,price_type,region,ipl,cluster_uckey,price_cat,ratio,imp,cluster_imp\n')
        for row in df:
            uckey = row['uckey']
            price_cat = row['price_cat']
            cluster_uckey = row['cluster_uckey']
            imp = row['imp']
            ratio = row['ratio']
            cluster_imp = row['cluster_imp']
            
            # print('{}  {}  {}'.format(uckey, price_cat, imp))
            uckeys.add(uckey)

            f.write('\"{}\",{},\"{}\",{},{},{},{}\n'.format(uckey, uckey, cluster_uckey, price_cat, ratio, imp, cluster_imp))
            count += 1
        return count, uckeys

def write_uckey_txt(uckeys, filepath):
    with open(filepath, 'w') as f:
        count = 0
        for uckey in uckeys:
            count += 1
            f.write('{}\n'.format(uckey))
            # f.write('\"{}\", '.format(uckey))
            # if count % 3 == 0:
            #     f.write('\n')

def run(cfg):
    sc = SparkContext()
    hive_context = HiveContext(sc)
    sc.setLogLevel(cfg['log_level'])

    # Load the distribution detail table.
    distribution_table = cfg['distribution_table']
    command = 'select * from {}'.format(distribution_table)
    df = hive_context.sql(command)

    # Calculate the total impressions by adding up the imp column.
    total = df.rdd.map(lambda x: (1,x[3])).reduceByKey(lambda x, y: x + y).collect()[0][1]

    # Get the dense uckeys.
    df_dense = df.filter(df['ratio'] == 1)

    # Write the dense uckeys to file.
    dense_count, dense_uckeys = write_uckey_csv(df_dense.collect(), 'dense_uckeys.csv')

    # Write just the dense uckey names to file.
    write_uckey_txt(dense_uckeys, 'dense_uckeys.txt')
    
    # Calculate the total number of impressions in the dense (non-virtual) uckeys.
    df_dense = df_dense.select('ratio', 'cluster_imp')
    dense_total = df_dense.rdd.reduceByKey(lambda x, y: x + y).collect()[0][1]

    # Get the virtual uckeys.
    df_virtual = df.filter(df['ratio'] < 1)

    # Write the virtual uckeys to file.
    virtual_count, virtual_uckeys = write_uckey_csv(df_virtual.collect(), 'virtual_uckeys.csv')

    # Write just the dense uckey names to file.
    write_uckey_txt(virtual_uckeys, 'virtual_uckeys.txt')
    
    # Calculate the total number of impressions of virtual uckeys.
    df_virtual = df_virtual.drop_duplicates(['cluster_uckey', 'price_cat']).select('cluster_imp')
    virtual_total = df_virtual.rdd.map(lambda x: (1,x[0])).reduceByKey(lambda x, y: x + y).collect()[0][1]

    sc.stop()

    # Print the results.
    print('Dense uckey count:   {:>7}  {:>5.1f}%'.format(dense_count, float(dense_count * 100)/(dense_count + virtual_count)))
    print('Virtual uckey count: {:>7}  {:>5.1f}%'.format(virtual_count, float(virtual_count * 100)/(dense_count + virtual_count)))

    print('Dense impression total:    {:>8}'.format(dense_total))
    print('Virtual impression total:  {:>8}'.format(virtual_total))
    print('Combined impression total: {:>8}   {:>8}'.format(total, dense_total + virtual_total))
    print('Dense impression %:   {:>5.1f}%'.format(float(dense_total)*100/(dense_total + virtual_total)))
    print('Virtual impression %: {:>5.1f}%'.format(float(virtual_total)*100/(dense_total + virtual_total)))


if __name__ == '__main__':

    cfg = {'distribution_table': 'dlpm_03182021_tmp_distribution_detail',
           'log_level': 'WARN'
           }

    run(cfg)





