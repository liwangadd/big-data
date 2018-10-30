package cn.windylee.curator.modeled;

public class PersonModel {

    private final PersonId id;
    private final ContainerType containerType;
    private final String firstName;
    private final String lastName;
    private final int age;

    public PersonModel() {
        this(new PersonId(), new ContainerType(), null, null, 0);
    }

    public PersonModel(PersonId id, ContainerType containerType, String firstName, String secondName, int age) {
        this.id = id;
        this.containerType = containerType;
        this.firstName = firstName;
        this.lastName = secondName;
        this.age = age;
    }

    public PersonId getId() {
        return id;
    }

    public ContainerType getContainerType() {
        return containerType;
    }

    public String getFirstName() {
        return firstName;
    }

    public String getLastName() {
        return lastName;
    }

    public int getAge() {
        return age;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        PersonModel that = (PersonModel) o;

        if (age != that.age) {
            return false;
        }
        if (!id.equals(that.id)) {
            return false;
        }
        if (!containerType.equals(that.containerType)) {
            return false;
        }
        //noinspection SimplifiableIfStatement
        if (!firstName.equals(that.firstName)) {
            return false;
        }
        return lastName.equals(that.lastName);
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + containerType.hashCode();
        result = 31 * result + firstName.hashCode();
        result = 31 * result + lastName.hashCode();
        result = 31 * result + age;
        return result;
    }

    @Override
    public String toString() {
        return "PersonModel{" + "id=" + id + ", containerType=" + containerType + ", firstName='" + firstName + '\'' +
                ", lastName='" + lastName + '\'' + ", age=" + age + '}';
    }
}
