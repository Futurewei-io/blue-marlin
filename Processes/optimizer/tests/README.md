### What is optimizer test?
Optimizer test is a testing program to test the optimizer program, especially for  booking buckets split and resources allocation. Optimizer test also tests performance of optimizer.

### Prerequisites
Cluster: Spark 2.3/HDFS 2.7/YARN 2.3/MapReduce 2.7/Hive 1.2.
Driver: Python 3.6, Spark Client 2.3.
Same as Optimizer.

### Install and Run
1.	Download the blue-martin/optomizer project
2.	Transfer the optomizer directory to ~/code/optimizer on a machine which also has Spark Client.
3.  cd optomizer.
4.  pip install -r requirements.txt (to install required packages).
5.  python setup.py install (to install optimizer package).
6.	Run 'python testing_file.py -v' under the optimizer directory.

### Documentation
Documentation is provided through comments in testing files and README files.

1. test_query_builder.py:  for testing that query_builder.py.
2. unit_test_split.py: for testing split_bbs() method.
3. unit_test_hwm_natural_order.py:  for testing hwm_allocation() method with naturual order.
4. unit_test_hwm.py:  for testing hwm_allocation() with bookings sorted by descending amount and descending bk_id.
5. unit_test_sorting.py: for testing sorting bookings method.
6. performance_test.py: for testing the performance for split_bbs and hws_allocation.

### How to check testing status?
1. The default testing result is commented at the bottom of each testing file.
2. Pay attention to the Fails. 'OK' confirms the testing case passed.
3. There is a total statistics after the testing program ends.

### How to customize or add testing cases?
1. Add more testing methods starting with 'test_' in each testing file.
2. Prepare the test data and the expected result, then asset a comparison.
3. Suggest to use Pandas and its assert_frame_equal() to compare pyspark data frames.
4. Functions starting with 'test_' will be automatically loaded and tested.
