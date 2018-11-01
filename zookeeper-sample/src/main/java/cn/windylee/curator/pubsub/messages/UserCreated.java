package cn.windylee.curator.pubsub.messages;

import cn.windylee.curator.pubsub.model.Message;
import cn.windylee.curator.pubsub.model.Priority;

public class UserCreated extends Message {

    private final String name;
    private final String position;

    public UserCreated() {
        this(Priority.low, "", "");
    }

    public UserCreated(Priority priority, String name, String position) {
        super(priority);
        this.name = name;
        this.position = position;
    }

    public UserCreated(String id, Priority priority, String name, String position) {
        super(id, priority);
        this.name = name;
        this.position = position;
    }

    public String getName() {
        return name;
    }

    public String getPosition() {
        return position;
    }

    @Override
    public String toString() {
        return "UserCreated{" + "name='" + name + '\'' + ", position='" + position + '\'' + "} " + super.toString();
    }
}
