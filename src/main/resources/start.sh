spark-submit --class dealLog.DealLogParseByState \
--jars $(echo ./lib/*.jar |tr ' '  ',') \
--master yarn \
--deploy-mode cluster \
--driver-memory 1g \
--executor-memory 2g \
--executor-cores 1 \
--num-executors 3 \
--files ./log4j.properties \
./calc_stream.jar \

