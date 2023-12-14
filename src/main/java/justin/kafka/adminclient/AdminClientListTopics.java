package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.ListTopicsResult;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class AdminClientListTopics extends AdminClientBase {

    public void execute(AdminClient admin) {
        ListTopicsResult topics = admin.listTopics();
        try {
            topics.names().get().forEach(System.out::println);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        AdminClientListTopics client = new AdminClientListTopics();
        client.run();
    }

}
