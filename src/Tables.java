/**
 * @author Eric Fahey <eric.fahey@ryerson.ca>
 */
public enum Tables {

    CENTRAL_STUDENT(new String[]{"studentNumber", "studentName", "degree", "gpa"}),
    CENTRAL_PROFESSOR(new String[]{"professorName", "engOffice", "engPhone", "sciOffice", "sciPhone"}),
    CENTRAL_COURSE(new String[]{"courseNumber", "courseName", "credits"}),
    CENTRAL_TEACHES(new String[]{"courseNumber", "professorName", "term"}),
    CENTRAL_ENROLLED(new String[]{"courseNumber", "professorName", "studentNumber", "status"}),

    SCI_STUDENT(new String[]{"studentNumber", "studentName", "degree", "gpa"}),
    SCI_PROFESSOR(new String[]{"professorName", "sciOffice", "sciPhone"}),
    SCI_COURSE(new String[]{"courseNumber", "courseName", "credits"}),
    SCI_TEACHES(new String[]{"courseNumber", "professorName", "term"}),
    SCI_ENROLLED(new String[]{"courseNumber", "professorName", "studentNumber", "status"}),

    ENG_STUDENT(new String[]{"studentNumber", "studentName", "degree", "gpa"}),
    ENG_PROFESSOR(new String[]{"professorName", "engOffice", "engPhone"}),
    ENG_COURSE(new String[]{"courseNumber", "courseName", "credits"}),
    ENG_TEACHES(new String[]{"courseNumber", "professorName", "term"}),
    ENG_ENROLLED(new String[]{"courseNumber", "professorName", "studentNumber", "status"});

    private final String[] attributes;

    Tables(String[] attributes) {
        this.attributes = attributes;
    }

    public String[] getAttributes() {
        return attributes;
    }
}
