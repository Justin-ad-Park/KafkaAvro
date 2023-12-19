package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.MockAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

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
    }
}
