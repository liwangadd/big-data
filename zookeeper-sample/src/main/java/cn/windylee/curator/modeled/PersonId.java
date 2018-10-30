package cn.windylee.curator.modeled;

public class PersonId {

    private final String id;

    public PersonId(String id) {
        this.id = id;
    }

    public PersonId() {
        this("");
    }

    public String getId() {
        return id;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) return true;
        if (obj == null || obj.getClass() != getClass()) return false;
        PersonId that = (PersonId) obj;
        return id.equals(that.id);
    }

    @Override
    public int hashCode() {
        return id.hashCode();
    }

    @Override
    public String toString() {
        return "PersonId{id=" + id + "}";
    }
}
