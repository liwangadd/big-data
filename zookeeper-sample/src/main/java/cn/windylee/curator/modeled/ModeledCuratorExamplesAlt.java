package cn.windylee.curator.modeled;

import org.apache.curator.x.async.modeled.ModeledFramework;

import java.util.function.Consumer;

public class ModeledCuratorExamplesAlt {

    public static void createOrUpdate(PersonModelSpec modelSpec, PersonModel model) {
        ModeledFramework<PersonModel> resolved = modelSpec.resolved(model.getContainerType(), model.getId());
        resolved.set(model);
    }

    public static void readPerson(PersonModelSpec modelSpec, ContainerType containerType, PersonId id, Consumer<PersonModel> receiver) {
        ModeledFramework<PersonModel> resolved = modelSpec.resolved(containerType, id);
        resolved.read().whenComplete((person, exception) -> {
            if (exception != null) exception.printStackTrace();
            else receiver.accept(person);
        });
    }

}
