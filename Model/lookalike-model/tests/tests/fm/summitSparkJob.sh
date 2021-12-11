CMD=$@
if [ -z "$CMD" ]
then
      echo  "Please input spark script arguments"
      exit
fi

echo "Script/Command:        "$CMD
###################    Resource Arguments #############################
DRIVER_CORES=8
DRIVER_MEMORY=16G
EXECUTOR_MEMORY=16G
EXECUTOR_CORES=8
NUMBER_OF_EXECUTOR=50
MEMORY_OVERHEAD=5G
MAX_RESULT_SIZE=2G
#JARS=""
#FILES="/opt/scripts/gradient_boost_tree_regression_Spark.py"
#################  Do not edit these lines ################################
export PYTHONPATH=/usr/local/lib64/python3.6/
export PYSPARK_DRIVER_PYTHON=/usr/bin/python3.6
export PYSPARK_PYTHON=/usr/bin/python3.6

spark-submit --master yarn \
 --deploy-mode client \
 --verbose \
 --conf "spark.yarn.dist.archives=../py_d.zip#so" \
 --conf "spark.executorEnv.PYSPARK_PYTHON=$PYSPARK_PYTHON" \
 --conf "spark.executorEnv.PYTHONPATH=./so" \
 --conf "spark.ui.showConsoleProgress=true" \
 --conf "spark.yarn.executor.memoryOverhead=$MEMORY_OVERHEAD" \
 --conf "spark.driver.maxResultSize=$MAX_RESULT_SIZE" \
 --driver-cores $DRIVER_CORES \
 --driver-memory $DRIVER_MEMORY \
 --executor-memory $EXECUTOR_MEMORY \
 --executor-cores $EXECUTOR_CORES \
 --num-executors $NUMBER_OF_EXECUTOR \
$CMD
#$CMD >> $LOG 2>&1
