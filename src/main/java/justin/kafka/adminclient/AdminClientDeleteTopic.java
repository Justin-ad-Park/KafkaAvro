package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;

public class AdminClientDeleteTopic extends AdminClientBase {

    public void run() {
        final String TOPIC_NAME = "test_topic";

        final ArrayList<String> TOPIC_LIST = new ArrayList<>(Arrays.asList(TOPIC_NAME));

        DescribeTopicsResult demoTopic = admin.describeTopics(TOPIC_LIST);

        try {
            TopicDescription topicDescription = demoTopic.topicNameValues().get(TOPIC_NAME).get();

            System.out.println("==Topic " + TOPIC_NAME + " is exist.");
            System.out.println("==Description== \n" + topicDescription );

            System.out.println("해당 토픽을 정말로 삭제하시겠습니까?y/n");

            Scanner scanner = new Scanner(System.in);
            String s = scanner.nextLine();

            if(!s.equals("y")) {
                System.out.println("토픽 삭제가 취소 되었습니다.");
                return;
            }

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
    }

}
