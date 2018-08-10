package cn.windylee.curator.discovery;

public class InstanceDetails {

    private String description;

    public InstanceDetails() {
        this.description = "";
    }

    public InstanceDetails(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }
}
