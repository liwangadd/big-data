package cn.windylee.curator.modeled;

public class ContainerType {

    private final int typeId;

    public ContainerType(int typeId) {
        this.typeId = typeId;
    }

    public ContainerType() {
        this(0);
    }

    public int getTypeId() {
        return typeId;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ContainerType that = (ContainerType) obj;
        return typeId == that.typeId;
    }

    @Override
    public int hashCode() {
        return this.typeId;
    }

    @Override
    public String toString() {
        return "ContainerType{typeId=" + typeId + "}";
    }
}
