package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.verification.VerificationMode;

import java.util.Collections;
import java.util.concurrent.ExecutionException;

import static org.mockito.Mockito.*;

public class MockAdminClientTest {
    static AdminClient admin;

    @BeforeAll
    public static void setUp() {
        Node broker = new Node(0, "Localhost", 9092);
        MockAdminClient adminClient = new MockAdminClient(Collections.singletonList(broker), broker);

        admin = spy(adminClient);

        AlterConfigsResult emptyResult = mock(AlterConfigsResult.class);
        doReturn(KafkaFuture.completedFuture(null)).when(emptyResult).all();
        // doReturn(emptyResult).when(admin.incrementalAlterConfigs(any()));
    }

    @Test
    public void testCreateTestTopic() throws ExecutionException, InterruptedException {
        TopicCreator tc = new TopicCreator(admin);
        tc.maybeCreateTopic("test.is.a.test.topic");

        verify(admin, times(1)).createTopics(any());
    }

    @Test
    void testNotCreateTopic()  throws ExecutionException, InterruptedException {
        TopicCreator tc = new TopicCreator(admin);
        tc.maybeCreateTopic("not.a.test.topic");

        verify(admin, never()).createTopics(any());
    }
}
