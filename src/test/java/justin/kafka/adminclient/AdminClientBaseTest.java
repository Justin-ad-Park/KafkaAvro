package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.ElectionType;
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


    /// 토픽 생성 /////////////////////////////////
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

    /// 토픽 리스트 조회 ////////////////////////////////
    @Test
    @Order(2)
    void listTopics() throws ExecutionException, InterruptedException {
        adminClient.listTopics().names().get().forEach(System.out::println);

        Optional<String> topicName = adminClient.listTopics().names().get().stream().findAny();

        Assertions.assertNull(topicName.get());
    }


    /// 토픽 존재 확인 /////////////////////////////////
    @Test
    @Order(3)
    void existTopic() throws ExecutionException, InterruptedException {
        boolean existTestTopic = adminClient.existTopic(TOPIC_NAME);

        boolean existRandomTopic = adminClient.existTopic("random");

        Assertions.assertTrue(existTestTopic);
        Assertions.assertFalse(existRandomTopic);

    }

    /// 토픽 설명 확인 /////////////////////////////////
    @Test
    @Order(4)
    void DescribeTopic() throws ExecutionException, InterruptedException {
        KafkaFuture<TopicDescription> topicDesc = getTopicDescription(TOPIC_NAME);

        //파티션 수와 각 파티션 정보를 보유주는 예
        System.out.println("Partition number : " + topicDesc.get().partitions().size());

        System.out.println("Partition0 info. : " + topicDesc.get().partitions().get(0).toString());

        System.out.println("Partitions info. : ");
        topicDesc.get().partitions().forEach(System.out::println);

    }

    private KafkaFuture<TopicDescription> getTopicDescription(String topicName) {
        DescribeTopicsResult topicsResult = adminClient.admin.describeTopics(Arrays.asList(topicName));

        Map<String, KafkaFuture<TopicDescription>> topicDescMap = topicsResult.topicNameValues();

        KafkaFuture<TopicDescription> topicDesc = topicDescMap.get(topicName);
        return topicDesc;
    }

    @Test
    void close() {
    }



    /// 토픽 삭제 /////////////////////////////////
    @Test
    @Order(5)
    void deleteTopic() throws ExecutionException, InterruptedException {

        if( !adminClient.existTopic(TOPIC_NAME) )
            return;

        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopic(TOPIC_NAME);

        Assertions.assertNotNull(deleteTopicsResult);

    }

    /// 컨슈머 그룹 확인 - 단순 리스트업 수준 /////////////////////////////////
    @Test
    void listConsumerGroups() throws ExecutionException, InterruptedException {
        adminClient.admin.listConsumerGroups().valid().get().forEach(System.out::println);
    }


    /// 컨슈머 그룹 확인 - 상세 내역(Describe) /////////////////////////////////
    @Test
    void describeConsumerGroups() throws ExecutionException, InterruptedException {
        ConsumerGroupDescription groupDescription = adminClient.admin.describeConsumerGroups(List.of("test-group", "test-users-group"))
                .describedGroups().get("test-group").get();

        System.out.println(groupDescription);
    }

    /// 컨슈머 그룹의 오프셋 정보 확인(파티션별 커밋오프셋, 토픽, 최종 오프셋, 오프셋 차이) /////////////////////////////////
    @Test
    void printLatestOfOffsets() throws ExecutionException, InterruptedException {
        String consumerGroup = "test-users-group";


        Map<TopicPartition, OffsetAndMetadata> offsets = adminClient.getPartitionOffsetMetadataByConsumerGroup(consumerGroup);

        var lastestOffsets = adminClient.getTopicPartitionListByOffsetSpec(consumerGroup, OffsetSpecEnum.Latest);

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
     * 컨슈머 그룹의 오프셋을 earliest(가장 빠른=가장 오래된)로 변경하는 API 호출
     */
    @Test
    void resetConsumerGroupOffsetsToEarliest() {
        String consumerGroup = "test-users-group";

        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpecEnum.Earliest);
    }

    /**
     * 컨슈머 그룹의 오프셋을 xxx분 전으로 변경
     */
    @Test
    void resetConsumerGroupOffsetsByTimestamp() {
        String consumerGroup = "test-users-group";

        Long currTimestamp = System.currentTimeMillis();
        Long targetTimestamp = currTimestamp - (60 * 1000 * 200);    //60 * 1000 = 1분 * xxx = xxx분

        System.out.println("Target Timestamp : " + targetTimestamp);


        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));
    }

    /**
     * 컨슈머 그룹의 오프셋을 특정한 타임 스탬프로 변경
     */
    @Test
    void resetConsumerGroupOffsetsByTimestamp2() {
        String consumerGroup = "test-users-group";

        Long targetTimestamp = 1704239104236L;

        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));
    }



    /**
     * 컨슈머 그룹의 오프셋을 특정한 시간(년월일시분초(나노초))으로 지정
     */
    @Test
    void resetConsumerGroupOffsetsBySpecificTimestamp() {
        String consumerGroup = "test-users-group";

        Long targetTimestamp = getSpecifiedTimestamp(2023,12,18,11,30,0);

        adminClient.alterConsumerGroupOffsets(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));
    }

    /// 년월일시분초를 타임스탭프로 변경
    private long getSpecifiedTimestamp(int year, int month, int dayOfMonth, int hour, int minute, int second) {
        LocalDateTime dateTime = LocalDateTime.of(year, month, dayOfMonth, hour, minute, second);
        // 타임존 설정 (예: 시스템 기본 타임존)
        ZoneId zoneId = ZoneId.systemDefault();
        // ZonedDateTime을 사용하여 타임존을 적용
        ZonedDateTime zonedDateTime = ZonedDateTime.of(dateTime, zoneId);

        // 타임스탬프로 변환 (밀리초 단위)
        return zonedDateTime.toInstant().toEpochMilli();
    }

    /// 클러스터 정보 조회 ///////////////////////////////////////////
    @Test
    void describeCluster() throws ExecutionException, InterruptedException {
        DescribeClusterResult descCluster = adminClient.admin.describeCluster();

        System.out.println("Connected to cluster :" + descCluster.clusterId().get());
        System.out.println("The controller is:" + descCluster.controller().get());
        System.out.println("The brokers in the cluster are:");
        descCluster.nodes().get().forEach(node -> System.out.println(node));
    }


    ///파티션 추가(줄이는 것 불가능. 입력값은 최종 파티션 수) /////////////////////
    @Test
    void increasePartition() throws ExecutionException, InterruptedException {
        String topicName = "test_users";

        int targetPartitionSize = 3;

        adminClient.increasePartition(topicName, targetPartitionSize);

        KafkaFuture<TopicDescription> topicDesc = getTopicDescription(topicName);

        int partitionSize = topicDesc.get().partitions().size();

        //파티션 수와 각 파티션 정보를 보유주는 예
        System.out.println("Partition number : " + topicDesc.get().partitions().size());

        Assertions.assertEquals(targetPartitionSize, partitionSize);

    }

    /// 레코드 삭제(타임스탭프) -  이전값(Before) 삭제, 타임스탭프가 동일한 레코드는 삭제하지 않음 /////////////////////

    /**
     * 레코드 삭제(타임스탭프) -  이전값(Before) 삭제, 타임스탭프가 동일한 레코드는 삭제하지 않음
     * *** 주의 **************************************************************************
     *   컨슈머 그룹과 무관하게 레코드 자체가 삭제되기 때문에 다른 컨슈머 그룹에서도 더 이상 해당 레코드를 조회 못함
     *   테스트 레코드를 삭제하거나, 브로커 설정에 의해 아직 삭제되지 않은 레코드를 강제로 삭제해야 하는 경우에만 사용해야 함
     * **********************************************************************************
     *
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void deleteRecordsByBeforeOffset() throws ExecutionException, InterruptedException {
        String consumerGroup = "test-users-group";

        Long targetTimestamp = getSpecifiedTimestamp(2023,12,18,11,30,0);

        adminClient.deleteRecordsByBeforeOffset(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));

    }

    /// 레코드 삭제(년월일시분초) - 이전값(Before) 삭제, 타임스탭프가 동일한 레코드는 삭제하지 않음 /////////////////////
    @Test
    void deleteRecordsByTimestamp() throws ExecutionException, InterruptedException {
        String consumerGroup = "test-users-group";

        Long targetTimestamp = 1702861379929L;

        adminClient.deleteRecordsByBeforeOffset(consumerGroup, OffsetSpec.forTimestamp(targetTimestamp));

    }

    /**
     * 리더 선출
     * -- ElectionType.PREFERRED : 선호파티션 우선
     * -- ElectionType.UNCLEAN : 사용 불능 리더가 있고, 데이터가 없어서 리더가 될 수 없는 레플리카가 있더라도
     *  레플리카를 강제 재할당 (데이터 유실 발생 가능성 있음. Kafka 서비스 복구는 됨)
     */
    @Test
    void electLeadersByPartitionNo() {
        Set<TopicPartition> electableTopics = new HashSet<>();
        electableTopics.add(new TopicPartition(TOPIC_NAME, 0)); //0번 파티션 리더 재선출

        try {
            adminClient.admin.electLeaders(ElectionType.PREFERRED, electableTopics).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 전체 리더 재선출
     */
    @Test
    void electAllLeaders() {
        Set<TopicPartition> electableTopics = new HashSet<>();  //아무값도 채우지 않으면 전체 토픽, 전체 파티션에 대해 재선출

        try {
            adminClient.admin.electLeaders(ElectionType.PREFERRED, electableTopics).all().get();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    /**
     * 토픽을 브로커에 재할당한다.
     *  브로커 설정 정보 확인
     *  $ cat $CONFLUENT_HOME/etc/kafka/server.properties |grep broker.id
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    void listPartitionReassignments() throws ExecutionException, InterruptedException {
        Map<TopicPartition, Optional<NewPartitionReassignment>> reassignment = new HashMap<>();

        reassignment.put(new TopicPartition(TOPIC_NAME, 0),
                Optional.of(new NewPartitionReassignment(Arrays.asList(0))));   //토픽의 파티션 0을 브로커 0에 할당

        //reassignment.put(new TopicPartition(TOPIC_NAME, 0), Optional.of(new NewPartitionReassignment(Arrays.asList(0,1))));   //토픽의 0번 파티션을 0,1을 브로커에 할당

        adminClient.admin.alterPartitionReassignments(reassignment).all().get();

        // 진행중인 재할당을 보여준다.
        System.out.println(adminClient.admin.listPartitionReassignments().reassignments().get());

        var topicDesc = getTopicDescription(TOPIC_NAME).get();
        System.out.println(topicDesc);

    }



}