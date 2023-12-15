package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.internals.Topic;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;

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
    void getOffset() throws ExecutionException, InterruptedException {
        String consumerGroup = "test-users-group";

        // 컨슈머 그룹의 모든 오프셋(파티션별 오프셋) 목록을 가져온다.
        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.admin.listConsumerGroupOffsets(consumerGroup)
                .partitionsToOffsetAndMetadata().get();

        Map<TopicPartition, OffsetSpec> requestLatestOffsets = new HashMap<>();

        // 각 오프셋에 커밋값을 요청하기 위한 Map을 만든다.
        for(TopicPartition tp: offsets.keySet()) {
            requestLatestOffsets.put(tp, OffsetSpec.latest());
        }

        //위에서 만든 맵으로 커밋 오프셋 값을 요청한다.
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> latestOffsets =
                adminClient.admin.listOffsets(requestLatestOffsets).all().get();

        for(Map.Entry<TopicPartition, OffsetAndMetadata> e: offsets.entrySet()) {
            String topic = e.getKey().topic();
            int partition = e.getKey().partition();
            long committedOffset = e.getValue().offset();
            long latestoffset = latestOffsets.get(e.getKey()).offset();

            System.out.printf("Consumer Group: %s,\t committed offset: %d, \t, topic: %s, \t latestOffset: %d,\t behind records: %d\n"
                    , consumerGroup, committedOffset, topic, latestoffset, (latestoffset - committedOffset));
        }

    }

    /**
     * 컨슈머 그룹의 오프셋을 earliest로 변경하는 API 호출
     */
    @Test
    void resetConsumerGroupOffsetsToEarliest() {
        String consumerGroup = "test-users-group";

        // 컨슈머 그룹의 모든 오프셋(파티션별 오프셋) 목록을 가져온다.
        Map<TopicPartition, OffsetAndMetadata> offsets = null;
        try {
            offsets = adminClient.admin.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        Map<TopicPartition, OffsetSpec> requestEarliestOffsets = new HashMap<>();

        // 각 오프셋에 커밋값을 요청하기 위한 Map을 만든다.
        for(TopicPartition tp: offsets.keySet()) {
            requestEarliestOffsets.put(tp, OffsetSpec.earliest());
        }

        // 컨슈머 그룹의 모든 오프셋(파티션별 오프셋) 목록을 가져온다.
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> earliestOffsets = null;
        try {
            earliestOffsets = adminClient.admin.listOffsets(requestEarliestOffsets).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();

        // 각 오프셋에 커밋값을 요청하기 위한 Map을 만든다.
        for(Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e: earliestOffsets.entrySet()) {
            resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()) );
        }

        try {
            adminClient.admin.alterConsumerGroupOffsets(consumerGroup, resetOffsets).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

}