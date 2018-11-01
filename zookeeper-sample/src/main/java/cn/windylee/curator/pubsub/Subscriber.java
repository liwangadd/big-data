package cn.windylee.curator.pubsub;

import cn.windylee.curator.pubsub.messages.LocationAvailable;
import cn.windylee.curator.pubsub.messages.UserCreated;
import cn.windylee.curator.pubsub.model.*;
import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.cached.CachedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;

import java.util.Objects;

public class Subscriber {

    private final AsyncCuratorFramework client;

    public Subscriber(AsyncCuratorFramework client) {
        this.client = Objects.requireNonNull(client, "client cannot be null");
    }

    public CachedModeledFramework<LocationAvailable> startLocationAvailableSubscriber(Group group, Priority priority) {
        return startSubscriber(Clients.locationAvailableClient, group, priority);
    }

    public CachedModeledFramework<UserCreated> startUserCreatedSubscriber(Group group, Priority priority) {
        return startSubscriber(Clients.userCreatedClient, group, priority);
    }

    public CachedModeledFramework<Instance> startInstanceSubscriber(InstanceType type) {
        CachedModeledFramework<Instance> cachedClient = Clients.instanceClient.resolved(client, type).cached();
        cachedClient.start();
        return cachedClient;
    }

    public <T extends Message> CachedModeledFramework<T> startSubscriber(TypedModeledFramework2<T, Group, Priority> typedClient,
                                                                         Group group, Priority priority) {
        CachedModeledFramework<T> cachedClient = typedClient.resolved(client, group, priority).cached();
        cachedClient.start();
        return cachedClient;
    }
}
