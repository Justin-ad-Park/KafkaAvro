package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.ExecutionException;

public class AdminClientCreateTopic extends AdminClientBase {

    public void execute(AdminClient admin) {
        final String TOPIC_NAME = "test_topic";
        final int NUMBER_PARTITIONS = 1;
        final short REP_FACTOR = 1;

        final ArrayList<String> TOPIC_LIST = new ArrayList<>(Arrays.asList(TOPIC_NAME));

        DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);

        try {
            TopicDescription topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();

            System.out.println("==Topic is exist==\n" + topicDescription);

            return;

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException | NullPointerException e) {
            System.out.println("Topic " + TOPIC_NAME + " doesn't exist.\n Going to create it now...");
        }

        CreateTopicsResult newTopic = admin.createTopics(Collections.singletonList(
                new NewTopic(TOPIC_NAME, NUMBER_PARTITIONS, REP_FACTOR)
        ));

        System.out.println("Topic is created.");

        try {
            if(newTopic.numPartitions(TOPIC_NAME).get() != NUMBER_PARTITIONS) {
                System.out.println("Error : Topic has wrong number of partitions.");
                System.exit(-1);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

    }

    public static void main(String[] args) {
        AdminClientCreateTopic client = new AdminClientCreateTopic();
        client.run();
    }

}
