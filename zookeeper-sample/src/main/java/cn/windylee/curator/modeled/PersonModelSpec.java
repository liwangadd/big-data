package cn.windylee.curator.modeled;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;

public class PersonModelSpec {

    private final AsyncCuratorFramework client;
    private final ModelSpec<PersonModel> modelSpec;

    public PersonModelSpec(AsyncCuratorFramework client) {
        this.client = client;

        JacksonModelSerializer<PersonModel> serializer = JacksonModelSerializer.build(PersonModel.class);
        ZPath path = ZPath.parse("/example/{id}/path/{id}");
        modelSpec = ModelSpec.builder(path, serializer).build();
    }

    public ModeledFramework<PersonModel> resolved(ContainerType containerType, PersonId personId) {
        ModelSpec<PersonModel> resolved = modelSpec.resolved(containerType.getTypeId(), personId.getId());
        return ModeledFramework.wrap(client, resolved);
    }
}
