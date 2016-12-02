dateStr=`date +%Y%m%d`
yesterday=`date  +\%Y-\%m-\%d --date="-1 day"`
echo "dt is $yesterday"
unset BEE_PASS
source /home/mart_wzyf/data_dir/wangzhehan/runSpark.sh

input=$1
output=$2
numExecutors=$3
driverMemory=$4
executorMemory=$5
executorCores=$6
usercnt=$7
itemcnt=$8
numcount=$9

#atc=$actionType"_"$cate

#input="rec_als_train_"$atc
#output="rec_als_rec_"$atc

args=input#$input,output#$output,num#$numcount

appname="input_"$input"_output_"$output"_num_"$numExecutors"_DM_"$driverMemory"_EM_"$executorMemory"_EC_"$executorCores"_uc_"$usercnt"_ic_"$itemcnt
echo "============>$args"
echo "appname:"$appname

spark-submit \
--class com.jd.plist.spark.mllib.als.rec_plist_als_rec \
--name $appname \
--master yarn-cluster \
--num-executors $numExecutors \
--driver-memory $driverMemory \
--executor-memory $executorMemory \
--executor-cores $executorCores \
--queue root.bdp_jmart_wzyf_union.bdp_jmart_wzyf_spark \
--conf spark.args=$args \
--conf spark.memory.fraction=0.6 \
--conf spark.memory.storageFraction=0.4 \
./plist-spark-1.0.jar
