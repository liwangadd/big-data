package cn.windylee.curator.modeled;

import org.apache.curator.x.async.AsyncCuratorFramework;
import org.apache.curator.x.async.modeled.JacksonModelSerializer;
import org.apache.curator.x.async.modeled.ModelSpec;
import org.apache.curator.x.async.modeled.ModeledFramework;
import org.apache.curator.x.async.modeled.ZPath;

import java.util.function.Consumer;

public class ModeledCuratorExamples {

    public static ModeledFramework<PersonModel> wrap(AsyncCuratorFramework client) {
        JacksonModelSerializer<PersonModel> serializer = JacksonModelSerializer.build(PersonModel.class);

        ModelSpec<PersonModel> modelSpec = ModelSpec.builder(ZPath.parse("/example/path"), serializer).build();

        return ModeledFramework.wrap(client, modelSpec);
    }

    public static void createOrUpdate(ModeledFramework<PersonModel> modeled, PersonModel model) {
        ModeledFramework<PersonModel> atId = modeled.child(model.getId().getId());
        atId.set(model);
    }

    public static void readPerson(ModeledFramework<PersonModel> modeled, String id, Consumer<PersonModel> receiver) {
        modeled.child(id).read().whenComplete((person, exception) -> {
            if (exception != null) exception.printStackTrace();
            else receiver.accept(person);
        });
    }

}
