package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;
import scala.Option;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ExecutionException;

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

        Optional<String> findTopicName = topicsList.names().get().stream().filter(x -> topicName.equals(x)).findAny();

        return !findTopicName.isEmpty();
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

}
