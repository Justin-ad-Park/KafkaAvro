package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

public class MockAdminClientSimpleTest {

    /**
     * 독자적인 Simple 테스트
     * @throws ExecutionException
     * @throws InterruptedException
     */
    @Test
    public void testCreateTopic() throws ExecutionException, InterruptedException {
        // MockAdminClient 인스턴스 생성
        Node broker = new Node(0, "Localhost", 9092);
        MockAdminClient mockAdminClient = new MockAdminClient(Collections.singletonList(broker), broker);

        // Spy를 사용하여 AdminClient 모의 객체 생성
        AdminClient spyAdminClient = spy(mockAdminClient);

        // 토픽 생성 요청
        NewTopic newTopic = new NewTopic("test.mock", 1, (short) 1);
        spyAdminClient.createTopics(Collections.singleton(newTopic)).all().get();

        // createTopics 메소드 호출 검증
        verify(spyAdminClient, times(1)).createTopics(Collections.singleton(newTopic));

        verify(spyAdminClient, times(1)).createTopics(any());
    }

    @Test
    public void testDescribeCluster() {
        // 노드와 토픽을 설정합니다
        Node node0 = new Node(0, "localhost", 9092);
        Node node1 = new Node(1, "localhost", 9093);

        List<Node> nodeList = List.of(node0, node1);

        MockAdminClient mockAdminClient = new MockAdminClient(nodeList, node0);

        // AdminClient를 생성합니다
        AdminClient adminClient = spy(mockAdminClient);

        // 클러스터 정보를 조회합니다
        adminClient.describeCluster().nodes().whenComplete((nodes, throwable) -> {
            nodes.forEach(System.out::println);
            assertEquals(2, nodes.size());
        });

        // 필요한 경우 추가적인 테스트를 수행합니다

        // 리소스 해제
        adminClient.close();
    }
}
