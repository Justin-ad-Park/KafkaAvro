package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.concurrent.ExecutionException;

class AdminClientBaseTest {
    AdminClientBase adminClient ;
    String TOPIC_NAME = "test_topic1";

    AdminClientBaseTest() {
        adminClient = new AdminClientBase();
    }

    @Test
    void listTopics() throws ExecutionException, InterruptedException {
        Optional<String> topicName = adminClient.listTopics().names().get().stream().findAny();

        adminClient.listTopics().names().get().forEach(System.out::println);

        Assertions.assertNotNull(topicName.get());
    }

    @Test
    void existTopic() throws ExecutionException, InterruptedException {
        boolean existTestTopic = adminClient.existTopic(TOPIC_NAME);

        boolean existRandomTopic = adminClient.existTopic("random");

        Assertions.assertTrue(existTestTopic);
        Assertions.assertFalse(existRandomTopic);

    }

    @Test
    void DescribeTopic() throws ExecutionException, InterruptedException {
        DescribeTopicsResult topicsResult = adminClient.admin.describeTopics(Arrays.asList("test"));

        Map<String, KafkaFuture<TopicDescription>> topicDescMap = topicsResult.topicNameValues();

        KafkaFuture<TopicDescription> topicDesc = topicDescMap.get("test");

        //파티션 수와 각 파티션 정보를 보유주는 예
        System.out.println("Partition number : " + topicDesc.get().partitions().size());

        System.out.println("Partition0 info. : " + topicDesc.get().partitions().get(0).toString());

        System.out.println("Partitions info. : ");
        topicDesc.get().partitions().forEach(System.out::println);


    }

    @Test
    void close() {
    }

    @Test
    @Order(1)
    void createTopic() throws ExecutionException, InterruptedException {

        if(adminClient.existTopic(TOPIC_NAME)) {
            System.out.println("Topic is already exist.");

            adminClient.deleteTopic(TOPIC_NAME);
        }

        CreateTopicsResult topicResult = adminClient.createTopic(TOPIC_NAME, 1, (short) 1);

        Uuid topicId = topicResult.topicId(TOPIC_NAME).get();

        Assertions.assertNotNull(topicId);

    }

    @Test
    @Order(2)
    void deleteTopic() throws ExecutionException, InterruptedException {

        if( !adminClient.existTopic(TOPIC_NAME) )
            return;

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopic(TOPIC_NAME);

        Assertions.assertNotNull(deleteTopicsResult);

    }

    @Test
    void listConsumerGroups() throws ExecutionException, InterruptedException {
        adminClient.admin.listConsumerGroups().valid().get().forEach(System.out::println);
    }

    @Test
    void describeConsumerGroups() throws ExecutionException, InterruptedException {
        ConsumerGroupDescription groupDescription = adminClient.admin.describeConsumerGroups(List.of("test-group", "test-users-group"))
                .describedGroups().get("test-group").get();

        System.out.println(groupDescription);
    }

    @Test
    void printLatestOfOffsets() throws ExecutionException, InterruptedException {
        String consumerGroup = "test-users-group";


        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.getPartitionOffsetMetadataByConsumerGroup(consumerGroup);

        var lastestOffsets = adminClient.getTopicPartitionListByOffsetSpec(offsets, OffsetSpecEnum.Latest);

        for(Map.Entry<TopicPartition, OffsetAndMetadata> e: offsets.entrySet()) {
            String topic = e.getKey().topic();
            int partition = e.getKey().partition();
            long committedOffset = e.getValue().offset();
            long latestOffset = lastestOffsets.get(e.getKey()).offset();

            System.out.printf("Consumer Group: %s,\t partiton: %d, \t committed offset: %d, \t topic: %s, \t latestOffset: %d,\t behind records: %d\n"
                    , consumerGroup, partition, committedOffset, topic, latestOffset, (latestOffset - committedOffset));
        }

    }

    /**
     * 컨슈머 그룹의 오프셋을 earliest로 변경하는 API 호출
     */
    @Test
    void resetConsumerGroupOffsetsToEarliest() {
        String consumerGroup = "test-users-group";

        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpecEnum.Earliest);
    }

    /**
     * 컨슈머 그룹의 오프셋을 5분 전으로 변경
     */
    @Test
    void resetConsumerGroupOffsetsByTimestamp() {
        String consumerGroup = "test-users-group";

        /* 특정 일시 설정 (예: 2023년 12월 18일 오후 3시 45분 30초)
        LocalDateTime dateTime = LocalDateTime.of(2023, 12, 18, 15, 45, 30);
        */

        Long currTimestamp = System.currentTimeMillis();
        Long targetTimestamp = currTimestamp - (60 * 1000 * 200);    //60 * 1000 = 1분 * xxx = xxx분

        System.out.println("Target Timestamp : " + targetTimestamp);


        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));
    }

    /**
     * 컨슈머 그룹의 오프셋을 특정한 시간으로 지정
     */
    @Test
    void resetConsumerGroupOffsetsBySpecificTimestamp() {
        String consumerGroup = "test-users-group";

        Long targetTimestamp = getSpecifiedTimestamp(2023,12,18,11,30,0);

        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));
    }

    private long getSpecifiedTimestamp(int year, int month, int dayOfMonth, int hour, int minute, int second) {
        LocalDateTime dateTime = LocalDateTime.of(year, month, dayOfMonth, hour, minute, second);
        // 타임존 설정 (예: 시스템 기본 타임존)
        ZoneId zoneId = ZoneId.systemDefault();
        // ZonedDateTime을 사용하여 타임존을 적용
        ZonedDateTime zonedDateTime = ZonedDateTime.of(dateTime, zoneId);

        // 타임스탬프로 변환 (밀리초 단위)
        return zonedDateTime.toInstant().toEpochMilli();
    }

}