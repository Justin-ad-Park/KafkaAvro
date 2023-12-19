package justin.kafka.adminclient;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class TopicCreator {
    AdminClient admin;

    public TopicCreator(AdminClient admin) {
        this.admin = admin;
    }

    public void maybeCreateTopic(String topicName) throws ExecutionException, InterruptedException {
        Collection<NewTopic> topics = new ArrayList<>();
        topics.add(new NewTopic(topicName, 1, (short) 1));

        //"test"로 시작하는 토픽만 생성한다.
        if(!topicName.toLowerCase().startsWith("test")) return;

        admin.createTopics(topics);

        ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topicName);

        ConfigEntry compaction = new ConfigEntry(TopicConfig.CLEANUP_POLICY_CONFIG,
                TopicConfig.CLEANUP_POLICY_COMPACT);

        Collection<AlterConfigOp> configOp = new ArrayList<>();
        configOp.add(new AlterConfigOp(compaction, AlterConfigOp.OpType.SET));

        Map<ConfigResource, Collection<AlterConfigOp>> alterConf = new HashMap<>();
        alterConf.put(configResource, configOp);
        admin.incrementalAlterConfigs(alterConf).all().get();
    }
}
