package dealLog;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryUntilElapsed;
import org.apache.curator.utils.ZKPaths;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.HasOffsetRanges;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.apache.spark.streaming.kafka010.OffsetRange;

public class DealLogConsumer {
    private static String brokers = "10.20.32.65:9092";
    private static String topics = "deal_log_test";
    private static String groupId = "deal_log_group_1";
    private static String zkServers = "10.20.32.65:2181";
    private static CuratorFramework  curatorFramework;

    public static void main(String[] args) throws InterruptedException {
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("deal log parse");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(1));

        Collection<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
        curatorFramework = connZk(zkServers);
        curatorFramework.start();

        Map<TopicPartition, Long> offsets = getZKOffsets(curatorFramework,groupId,topics);

        JavaInputDStream<ConsumerRecord<Object,Object>> lines = KafkaUtils.createDirectStream(
                ssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, getKafkaParam(), offsets)
        );

        lines.foreachRDD(rdd -> {
            rdd.foreach(x -> {
                System.out.println(x.value());
            });
        });

        saveZKOffset(curatorFramework,groupId,lines);

        ssc.start();
        ssc.awaitTermination();

        curatorFramework.close();
        ssc.close();
    }
    private static Map<String, Object> getKafkaParam() {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", brokers);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", groupId);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        return kafkaParams;
    }

    private static CuratorFramework connZk(String zkServers) {
        curatorFramework = CuratorFrameworkFactory.builder()
                .connectString(zkServers).connectionTimeoutMs(1000)
                .sessionTimeoutMs(10000).retryPolicy(new RetryUntilElapsed(1000, 1000)).build();
        try {
            Thread.sleep(1500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        return curatorFramework;
    }

    private static Map<TopicPartition,Long> getZKOffsets(CuratorFramework  curatorFramework,String groupId, String topic) {
        Map<TopicPartition,Long> latestOffsets = new HashMap();

        try{
            List<String> partList = curatorFramework.getChildren().forPath("/brokers/topics/" + topic + "/partitions");
            partList.forEach(p ->{
                int pNum = Integer.parseInt(p);
                TopicPartition topicAndPartition = new TopicPartition(topic,pNum);

                String tpPath = "/consumers/" + groupId + "/offsets/" + topic + "/" + pNum;
                try {
                    if (curatorFramework.checkExists().forPath(tpPath) == null) {
                        System.out.println("partion num==" + pNum + " offset==" + 0);
                        latestOffsets.put(topicAndPartition, 0l);

                        ZKPaths.mkdirs(curatorFramework.getZookeeperClient().getZooKeeper(), tpPath);
                        curatorFramework.setData().forPath(tpPath, "0".getBytes());
                    }else {
                        byte[] partitionOffset = curatorFramework.getData().forPath(tpPath);
                        latestOffsets.put(topicAndPartition, Long.parseLong(new String(partitionOffset)));

                        System.out.println("partion num==" + pNum + " offset==" +  Long.parseLong(new String(partitionOffset)));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            });
        }catch(Exception e){
            e.printStackTrace();
        }

        return latestOffsets;
    }

    private static void saveZKOffset(CuratorFramework curatorFramework,String groupId,JavaInputDStream<ConsumerRecord<Object,Object>> stream) {
        stream.foreachRDD(recordRDD -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) recordRDD.rdd()).offsetRanges();
            if (!recordRDD.isEmpty()) {
                for(int i = 0;i < offsetRanges.length;i++) {
                    OffsetRange offsetRange = offsetRanges[i];
                    curatorFramework.setData().forPath("/consumers/" + groupId + "/offsets/" + offsetRange.topic() + "/" + offsetRange.partition(),
                            String.valueOf(offsetRange.untilOffset()).getBytes());
                }
            }
        });
    }
}
