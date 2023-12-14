package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public abstract class AdminClientBase {

    abstract void execute(AdminClient admin);

    public void run() {
        Properties props = new Properties();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        AdminClient admin = AdminClient.create(props);

        //AdminClient에 필요한 작업을 수행한다.
        execute(admin);

        admin.close(Duration.ofSeconds(30));
    }

}
