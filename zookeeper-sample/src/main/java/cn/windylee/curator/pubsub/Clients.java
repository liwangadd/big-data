package cn.windylee.curator.pubsub;

import cn.windylee.curator.pubsub.messages.LocationAvailable;
import cn.windylee.curator.pubsub.messages.UserCreated;
import cn.windylee.curator.pubsub.model.Group;
import cn.windylee.curator.pubsub.model.Instance;
import cn.windylee.curator.pubsub.model.InstanceType;
import cn.windylee.curator.pubsub.model.Priority;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModelSpecBuilder;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework;
import org.apache.curator.x.async.modeled.typed.TypedModeledFramework2;
import org.apache.zookeeper.CreateMode;

import java.util.concurrent.TimeUnit;

public class Clients {

    public static final TypedModeledFramework2<LocationAvailable, Group, Priority> locationAvailableClient =
            TypedModeledFramework2.from(ModeledFramework.builder(), builder(LocationAvailable.class),
                    "/windylee/pubsub/messages/locations/{group}/{priority}/{id}");

    public static final TypedModeledFramework2<UserCreated, Group, Priority> userCreatedClient = TypedModeledFramework2
            .from(ModeledFramework.builder(), builder(UserCreated.class),
                    "/windylee/pubsub/messages/users/{group}/{priority}/{id}");

    public static final TypedModeledFramework<Instance, InstanceType> instanceClient = TypedModeledFramework.from(
            ModeledFramework.builder(), builder(Instance.class), "/windylee/pubsub/{instance-type}/{id}"
    );

    private static <T> ModelSpecBuilder<T> builder(Class<T> clazz) {
        return ModelSpec.builder(JacksonModelSerializer.build(clazz))
                .withTtl(TimeUnit.MINUTES.toMicros(10))
                .withCreateMode(CreateMode.PERSISTENT_WITH_TTL);
    }

}
