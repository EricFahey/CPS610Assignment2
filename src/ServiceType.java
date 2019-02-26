/**
 * @author Eric Fahey <eric.fahey@ryerson.ca>
 */
public enum ServiceType {

    XE("xe");

    private final String sid;

    ServiceType(String sid) {
        this.sid = sid;
    }

    public String getSid() {
        return sid;
    }
}
