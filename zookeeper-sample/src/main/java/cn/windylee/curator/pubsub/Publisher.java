package cn.windylee.curator.pubsub;

import cn.windylee.curator.pubsub.messages.LocationAvailable;
import cn.windylee.curator.pubsub.messages.UserCreated;
import cn.windylee.curator.pubsub.model.Group;
import cn.windylee.curator.pubsub.model.Instance;
import cn.windylee.curator.pubsub.model.Message;
import cn.windylee.curator.pubsub.model.Priority;
import org.apache.curator.framework.api.transaction.CuratorOp;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Publisher {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final AsyncCuratorFramework client;

    public Publisher(AsyncCuratorFramework client) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
    }

    public void publishInstance(Instance instance) {
        ModeledFramework<Instance> instanceClient = Clients.instanceClient.resolved(client, instance.getType());
        instanceClient.set(instance).exceptionally(e -> {
            log.error("Could not publish instance: " + instance, e);
            return null;
        });
    }

    public void publishInstances(List<Instance> instances) {
        List<CuratorOp> operations = instances.stream().map(instance -> Clients.instanceClient
                .resolved(client, instance.getType()).createOp(instance))
                .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish instances: " + instances, e);
            return null;
        });
    }

    public void publishLocationAvailable(Group group, LocationAvailable locationAvailable) {
        publishMessage(Clients.locationAvailableClient, group, locationAvailable);
    }

    public void publishUserCreated(Group group, UserCreated userCreated) {
        publishMessage(Clients.userCreatedClient, group, userCreated);
    }

    public void publishLocationAvailable(Group group, List<LocationAvailable> locationAvailables) {
        publishMessages(Clients.locationAvailableClient, group, locationAvailables);
    }

    public void publishUsersCreated(Group group, List<UserCreated> usersCreated) {
        publishMessages(Clients.userCreatedClient, group, usersCreated);
    }

    private <T extends Message> void publishMessage(TypedModeledFramework2<T, Group, Priority> typedClient, Group group,
                                                    T message) {
        ModeledFramework<T> resolvedClient = typedClient.resolved(client, group, message.getPriority());
        resolvedClient.set(message).exceptionally(e -> {
            log.error("Could not publish message: " + message);
            return null;
        });
    }

    private <T extends Message> void publishMessages(TypedModeledFramework2<T, Group, Priority> typedClient, Group group,
                                                     List<T> messages) {
        List<CuratorOp> operations = messages.stream().map(message -> typedClient.resolved(client, group, message.getPriority()).createOp(message))
                .collect(Collectors.toList());
        client.transaction().forOperations(operations).exceptionally(e -> {
            log.error("Could not publish messages: " + messages, e);
            return null;
        });
    }

}
