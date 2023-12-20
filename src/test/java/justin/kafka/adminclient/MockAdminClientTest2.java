package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.spy;

public class MockAdminClientTest2 {

    private MockAdminClient mockAdminClient;
    private AdminClient adminClient;

    @BeforeEach
    public void setUp() {
        // 클러스터 설정
        Node node = new Node(0, "localhost", 9092);
        mockAdminClient = new MockAdminClient(Collections.singletonList(node), node);

        // AdminClient 초기화
        adminClient = spy(mockAdminClient);
    }

    @Test
    public void testCreateListAndDeleteTopics() throws ExecutionException, InterruptedException {
        // 토픽 생성
        String topicName = "test-topic";
        NewTopic newTopic = new NewTopic(topicName, Optional.of(1), Optional.of((short) 1));
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createTopicsResult.all().get();

        // 토픽 리스트 확인
        ListTopicsResult listTopicsResult = adminClient.listTopics();
        Set<String> topics = listTopicsResult.names().get();
        assertTrue(topics.contains(topicName));

        // 토픽 삭제
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(Collections.singletonList(topicName));
        deleteTopicsResult.all().get();

        // 삭제 후 토픽 리스트 확인
        topics = adminClient.listTopics().names().get();
        assertFalse(topics.contains(topicName));
    }

    @Test
    public void testDescribeTopic() throws ExecutionException, InterruptedException {
        // 토픽 생성
        String topicName = "describe-topic";
        NewTopic newTopic = new NewTopic(topicName, Optional.of(1), Optional.of((short) 1));
        adminClient.createTopics(Collections.singletonList(newTopic)).all().get();

        // 토픽 설명 확인
        DescribeTopicsResult describeTopicsResult = adminClient.describeTopics(Collections.singletonList(topicName));
        Map<String, TopicDescription> topicDescriptionMap = describeTopicsResult.all().get();
        assertTrue(topicDescriptionMap.containsKey(topicName));
        assertEquals(1, topicDescriptionMap.get(topicName).partitions().size());

        // 토픽 삭제
        adminClient.deleteTopics(Collections.singletonList(topicName)).all().get();
    }

    @Test
    public void testAlterTopicConfigurations() throws ExecutionException, InterruptedException {
        // 토픽 생성
        String topicName = "test-topic";
        NewTopic newTopic = new NewTopic(topicName, Optional.of(1), Optional.of((short) 1));
        CreateTopicsResult createTopicsResult = adminClient.createTopics(Collections.singletonList(newTopic));
        createTopicsResult.all().get();

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);


        // 토픽의 설정 내역 확인
        DescribeConfigsResult beforeDescribeConfigsResult = adminClient.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
        Map<ConfigResource, Config> beforeDescribedConfigs = beforeDescribeConfigsResult.all().get();

        // 설정 전에는 엔트리 0개
        assertEquals(0, beforeDescribedConfigs.get(configResource).entries().size() );


        // [config 추가 ------------------
        Collection<AlterConfigOp> configOp = new ArrayList<>();

        // CleanUp Policy 추가
        ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT);
        configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));

        // Retention ms 추가
        String retention_ms = "7200000";
        ConfigEntry retentionMs = new ConfigEntry(TopicConfig.RETENTION_MS_CONFIG, retention_ms);
        configOp.add(new AlterConfigOp(retentionMs, AlterConfigOp.OpType.SET));


        Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
        alterConf.put(configResource, configOp);

        AlterConfigsResult alterConfigsResult = adminClient.incrementalAlterConfigs(alterConf);
        alterConfigsResult.all().get();

        //------------------------- config 추가]


        // 변경 사항 확인
        DescribeConfigsResult describeConfigsResult = adminClient.describeConfigs(Collections.singletonList(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
        Map<ConfigResource, Config> describedConfigs = describeConfigsResult.all().get();

        System.out.println(describedConfigs.get(new ConfigResource(ConfigResource.Type.TOPIC, topicName)));
        System.out.println(TopicConfig.CLEANUP_POLICY_CONFIG + " : " + describedConfigs.get(configResource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());
        System.out.println(TopicConfig.RETENTION_MS_CONFIG + " : " + describedConfigs.get(configResource).get(TopicConfig.RETENTION_MS_CONFIG).value());


        // 엔트리 2개
        assertEquals(2, describedConfigs.get(configResource).entries().size() );

        // 상세값 비교
        assertEquals(TopicConfig.CLEANUP_POLICY_COMPACT, describedConfigs.get(configResource).get(TopicConfig.CLEANUP_POLICY_CONFIG).value());
        assertEquals(retention_ms, describedConfigs.get(configResource).get(TopicConfig.RETENTION_MS_CONFIG).value());
    }

}
