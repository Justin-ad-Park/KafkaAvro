package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.checkerframework.checker.units.qual.K;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;


/**
 * https://kafka.apache.org/35/javadoc/org/apache/kafka/clients/admin/package-summary.html
 * https://kafka.apache.org/protocol.html#protocol_api_keys
 */
public class AdminClientBase {
    AdminClient admin;

    protected AdminClientBase() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        admin = AdminClient.create(props);

    }

    public final void close() {
        admin.close(Duration.ofSeconds(30));

    }

    public final ListTopicsResult listTopics() {
        return admin.listTopics();
    }

    public boolean existTopic(String topicName) throws ExecutionException, InterruptedException {
        ListTopicsResult topicsList = listTopics();

        Optional<String> foundTopicName = topicsList.names().get().stream().filter(topicName::equals).findAny();

        return foundTopicName.isPresent();
    }

    public CreateTopicsResult createTopic(String topicName, int numberPartitions, short repFactor) {

        CreateTopicsResult newTopic = admin.createTopics(
                Collections.singletonList(
                    new NewTopic(topicName, numberPartitions, repFactor)
                )
        );

        return newTopic;
    }

    public DeleteTopicsResult deleteTopic(String topicName) {

        final ArrayList<String> TOPIC_LIST = new ArrayList<>(Arrays.asList(topicName));

        return admin.deleteTopics(TOPIC_LIST);
    }

    /**
     * 컨슈머 그룹의 각 파티션별 오프셋 정보
     * @param consumerGroup
     * @return
     */
    public Map<TopicPartition, OffsetAndMetadata> getPartitionOffsetMetadataByConsumerGroup(String consumerGroup) {
        // 컨슈머 그룹의 모든 오프셋(파티션별 오프셋) 목록을 가져온다.
        Map<TopicPartition, OffsetAndMetadata> offsets = null;
        try {
            offsets = admin.listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata().get();

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return offsets;
    }



    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getTopicPartitionListByOffsetSpec(String consumerGroup, OffsetSpecEnum offsetSpecEnum) {
        return getTopicPartitionListByOffsetSpec(consumerGroup, offsetSpecEnum.getOffsetSpec());
    }

    /**
     * 컨슈머 그룹의 각 파티션별 특정 오프셋 스펙 정보(Earliest, Latest, MaxTimeStamp)
     * @param consumerGroup
     * @param offsetSpec
     * @return
     */
    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getTopicPartitionListByOffsetSpec(String consumerGroup, OffsetSpec offsetSpec) {
        Map<TopicPartition, OffsetAndMetadata> offsets = getPartitionOffsetMetadataByConsumerGroup(consumerGroup);

        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> OffsetsResult = getTopicPartitionListByOffsetSpec(offsets, offsetSpec);
        return OffsetsResult;
    }

    public Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getTopicPartitionListByOffsetSpec(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetSpecEnum offsetSpecEnum) {
        return getTopicPartitionListByOffsetSpec(offsets, offsetSpecEnum.getOffsetSpec());
    }

    /**
     * 컨슈머 그룹의 각 파티션별 특정 오프셋 스펙 정보(Earliest, Latest, MaxTimeStamp)
     * @param offsets
     * @param offsetSpec
     * @return
     */
    private Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> getTopicPartitionListByOffsetSpec(Map<TopicPartition, OffsetAndMetadata> offsets, OffsetSpec offsetSpec) {
        Map<TopicPartition, OffsetSpec> requestOffsetSpecs = new HashMap<>();

        // 각 오프셋에 커밋값을 요청하기 위한 Map을 만든다.
        for(TopicPartition tp: offsets.keySet()) {
            requestOffsetSpecs.put(tp, offsetSpec);
        }

        // 컨슈머 그룹의 모든 오프셋(파티션별 오프셋) 목록을 가져온다.
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> OffsetsResult = null;
        try {
            OffsetsResult = admin.listOffsets(requestOffsetSpecs).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return OffsetsResult;
    }

    /**
     * 컨슈머 그룹의 오프셋을 변경한다.
     * @param consumerGroup
     * @param offsetSpecEnum (Earliest, Latest, MaxTimeStamp)
     */
    public void alterConsumerGroupOffsets(String consumerGroup, OffsetSpecEnum offsetSpecEnum) {
        alterConsumerGroupOffsets(consumerGroup, offsetSpecEnum.getOffsetSpec());
    }


    /**
     * 컨슈머 그룹의 오프셋을 변경한다.
     * @param consumerGroup
     * @param offsetSpec
     *  offsetSpec을 직접 지정
     *  특히, TimestampSpec로 세밀하게 오프셋을 변경할 때 사용
     *  예)
     *  alterConsumerGroupOffsets("comsumerGroupName",
     */
    public void alterConsumerGroupOffsets(String consumerGroup, OffsetSpec offsetSpec) {
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> OffsetsResult = getTopicPartitionListByOffsetSpec(consumerGroup, offsetSpec);

        Map<TopicPartition, OffsetAndMetadata> resetOffsets = new HashMap<>();

        // 각 오프셋에 커밋값을 요청하기 위한 Map을 만든다.
        for(Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e: OffsetsResult.entrySet()) {
            System.out.printf("Key: %s, offset: %s, timestamp: %d \n", e.getKey(), e.getValue().offset(), e.getValue().timestamp());
            resetOffsets.put(e.getKey(), new OffsetAndMetadata(e.getValue().offset()) );
        }

        try {
            admin.alterConsumerGroupOffsets(consumerGroup, resetOffsets).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public void increasePartition(String topicName, int partitionNumber) throws ExecutionException, InterruptedException {
        Map<String, NewPartitions> newPartitions = Map.of(topicName, NewPartitions.increaseTo(partitionNumber));

        admin.createPartitions(newPartitions).all().get();

    }

    public void deleteRecordsByBeforeOffset( String consumerGroup, OffsetSpecEnum offsetSpecEnum) throws ExecutionException, InterruptedException {
        deleteRecordsByBeforeOffset(consumerGroup, offsetSpecEnum.getOffsetSpec());
    }

    public void deleteRecordsByBeforeOffset( String consumerGroup, OffsetSpec offsetSpec ) throws ExecutionException, InterruptedException {
        var olderOffsets = getTopicPartitionListByOffsetSpec(consumerGroup, offsetSpec);

        Map<TopicPartition, RecordsToDelete> recordsToDelete = new HashMap<>();

        for(Map.Entry<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> e: olderOffsets.entrySet()) {
            recordsToDelete.put(e.getKey(), RecordsToDelete.beforeOffset(e.getValue().offset()));
        }

        admin.deleteRecords(recordsToDelete).all().get();
    }

}
