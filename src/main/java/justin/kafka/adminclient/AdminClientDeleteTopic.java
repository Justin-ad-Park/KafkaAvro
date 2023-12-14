package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class AdminClientDeleteTopic extends AdminClientBase {

    public void execute(AdminClient admin) {
        final String TOPIC_NAME = "test_topic";

        final ArrayList<String> TOPIC_LIST = new ArrayList<>(Arrays.asList(TOPIC_NAME));

        DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);

        try {
            TopicDescription topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();

            System.out.println("==Topic " + TOPIC_NAME + " is exist.");
            System.out.println("==Description== \n" + topicDescription );

            admin.deleteTopics(TOPIC_LIST);

            System.out.println("Topic is deleted.");

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException | NullPointerException e) {
            System.out.println("Topic is not exist.");
        }

    }

    public static void main(String[] args) {
        AdminClientDeleteTopic client = new AdminClientDeleteTopic();
        client.run();
    }

}
