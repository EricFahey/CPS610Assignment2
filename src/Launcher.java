import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Eric Fahey <eric.fahey@ryerson.ca>
 */
public class Launcher {

    public static DatabaseConnection central = new DatabaseConnection(
            "localhost", 1522, "SYSTEM", "admin", ServiceType.SE);

    public static DatabaseConnection science = central;

    public static DatabaseConnection engineering = new DatabaseConnection(
            "localhost", 1523, "SYSTEM", "admin", ServiceType.XE);

    public static void main(String[] args) {

        central.connect();
        engineering.connect();

        //Truncate Old Data
        truncateTables();

        //Populate Central Database
        populateCentralDatabase();

        //Distribute to Engineering/Science Sites
        populateFacultyDatabases();

        //Reconstruction
        reconstructionQueries();

        System.out.println("Finished!");

    }

    public static void truncateTables() {
        System.out.println("Truncating Tables!");
        central.truncate(Tables.CENTRAL_TEACHES);
        science.truncate(Tables.SCI_TEACHES);
        engineering.truncate(Tables.ENG_TEACHES);

        central.truncate(Tables.CENTRAL_ENROLLED);
        science.truncate(Tables.SCI_ENROLLED);
        engineering.truncate(Tables.ENG_ENROLLED);

        central.truncate(Tables.CENTRAL_COURSE);
        science.truncate(Tables.SCI_COURSE);
        engineering.truncate(Tables.ENG_COURSE);

        central.truncate(Tables.CENTRAL_STUDENT);
        science.truncate(Tables.SCI_STUDENT);
        engineering.truncate(Tables.ENG_STUDENT);

        central.truncate(Tables.CENTRAL_PROFESSOR);
        science.truncate(Tables.SCI_PROFESSOR);
        engineering.truncate(Tables.ENG_PROFESSOR);
    }

    public static void populateCentralDatabase() {
        System.out.println("Start Central Population!");
        central.insert(Tables.CENTRAL_STUDENT, "'500123456', 'Ryan Chong', 'Bachelors of Science', 3.0");
        central.insert(Tables.CENTRAL_STUDENT, "'500638753', 'Iria Pramoda', 'Bachelors of Science', 3.2");
        central.insert(Tables.CENTRAL_STUDENT, "'500623847', 'Azaan Major', 'Bachelors of Science', 2.5");
        central.insert(Tables.CENTRAL_STUDENT, "'500639428', 'Samiyah Fritz', 'Bachelors of Science', 3.4");
        central.insert(Tables.CENTRAL_STUDENT, "'500591847', 'Jude Perkins', 'Bachelors of Science', 2.8");
        central.insert(Tables.CENTRAL_STUDENT, "'500591238', 'Asher Carrillo', 'Bachelors of Science', 3.9");
        central.insert(Tables.CENTRAL_STUDENT, "'500634578', 'Nishat Gross', 'Bachelors of Engineering', 3.4");
        central.insert(Tables.CENTRAL_STUDENT, "'500645093', 'Laaibah Cooley', 'Bachelors of Engineering', 2.4");
        central.insert(Tables.CENTRAL_STUDENT, "'500630495', 'Mara Jeffery', 'Bachelors of Engineering', 3.8");
        central.insert(Tables.CENTRAL_STUDENT, "'500698342', 'Giovanni Kenny', 'Bachelors of Engineering', 2.8");
        central.insert(Tables.CENTRAL_STUDENT, "'500645930', 'Jordan Mccoy', 'Bachelors of Engineering', 3.3");

        central.insert(Tables.CENTRAL_PROFESSOR, "'Lilith Hart', 'ENG281', '416-555-1847', 'VIC601', '416-555-4987'");
        central.insert(Tables.CENTRAL_PROFESSOR, "'Kaylan Goulding', 'ENG282', '416-555-8729', 'VIC602', '416-555-5098'");
        central.insert(Tables.CENTRAL_PROFESSOR, "'Josie Mcneil', 'ENG283', '416-555-2783', 'VIC603', '416-555-0487'");

        central.insert(Tables.CENTRAL_COURSE, "'109', 'Computer Science 1', 1");
        central.insert(Tables.CENTRAL_COURSE, "'209', 'Computer Science 2', 1");
        central.insert(Tables.CENTRAL_COURSE, "'509', 'Engineering 1', 1");
        central.insert(Tables.CENTRAL_COURSE, "'609', 'Engineering 2', 1");

        central.insert(Tables.CENTRAL_TEACHES, "'109', 'Lilith Hart', 'W2019'");
        central.insert(Tables.CENTRAL_TEACHES, "'209', 'Kaylan Goulding', 'W2019'");
        central.insert(Tables.CENTRAL_TEACHES, "'509', 'Josie Mcneil', 'W2019'");
        central.insert(Tables.CENTRAL_TEACHES, "'609', 'Josie Mcneil', 'W2019'");

        central.insert(Tables.CENTRAL_ENROLLED, "'109', 'Lilith Hart', '500638753', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'109', 'Lilith Hart', '500623847', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'109', 'Lilith Hart', '500639428', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'109', 'Lilith Hart', '500645930', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'109', 'Lilith Hart', '500698342', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'209', 'Kaylan Goulding', '500591238', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'209', 'Kaylan Goulding', '500638753', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'209', 'Kaylan Goulding', '500623847', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'209', 'Kaylan Goulding', '500591847', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'209', 'Kaylan Goulding', '500630495', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'509', 'Josie Mcneil', '500634578', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'509', 'Josie Mcneil', '500645093', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'509', 'Josie Mcneil', '500630495', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'509', 'Josie Mcneil', '500639428', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'509', 'Josie Mcneil', '500591847', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'509', 'Josie Mcneil', '500645930', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'609', 'Josie Mcneil', '500634578', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'609', 'Josie Mcneil', '500645093', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'609', 'Josie Mcneil', '500698342', 'Enrolled'");
        central.insert(Tables.CENTRAL_ENROLLED, "'609', 'Josie Mcneil', '500591238', 'Enrolled'");
    }

    public static void populateFacultyDatabases() {
        ArrayList<HashMap<String, String>> data;
        System.out.println("Start Faculty Population!");
        //Students
        data = central.selectByConditions(Tables.CENTRAL_STUDENT, "Degree='Bachelors of Science'");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            science.insert(Tables.SCI_STUDENT, attributes, values);
        }
        data = central.selectByConditions(Tables.CENTRAL_STUDENT, "Degree='Bachelors of Engineering'");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            engineering.insert(Tables.ENG_STUDENT, attributes, values);
        }
        //Professors
        data = central.selectByAttributes(Tables.CENTRAL_PROFESSOR, String.join(",", Tables.SCI_PROFESSOR.getAttributes()));
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            science.insert(Tables.SCI_PROFESSOR, attributes, values);
        }
        data = central.selectByAttributes(Tables.CENTRAL_PROFESSOR, String.join(",", Tables.ENG_PROFESSOR.getAttributes()));
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            engineering.insert(Tables.ENG_PROFESSOR, attributes, values);
        }
        //Courses
        data = central.selectByConditions(Tables.CENTRAL_COURSE, "courseNumber<500");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            science.insert(Tables.SCI_COURSE, attributes, values);
        }
        data = central.selectByConditions(Tables.CENTRAL_COURSE, "courseNumber>=500");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            engineering.insert(Tables.ENG_COURSE, attributes, values);
        }
        //Teaches
        data = central.selectByConditions(Tables.CENTRAL_TEACHES, "courseNumber<500");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            science.insert(Tables.SCI_TEACHES, attributes, values);
        }
        data = central.selectByConditions(Tables.CENTRAL_TEACHES, "courseNumber>=500");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            engineering.insert(Tables.ENG_TEACHES, attributes, values);
        }
        //Enrolled
        data = central.selectByConditions(Tables.CENTRAL_ENROLLED, "courseNumber<500");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            science.insert(Tables.SCI_ENROLLED, attributes, values);
        }
        data = central.selectByConditions(Tables.CENTRAL_ENROLLED, "courseNumber>=500");
        for (HashMap<String, String> map : data) {
            String attributes = String.join(",", map.keySet());
            String values = String.join(",", map.values());
            engineering.insert(Tables.ENG_ENROLLED, attributes, values);
        }
    }

    public static void reconstructionQueries() {
        ArrayList<HashMap<String, String>> data;
        ArrayList<HashMap<String, String>> data2;
        System.out.println("----- ALL STUDENTS WHO HAVE OVER 3.5 GPA -----");
        data = science.select("SELECT * FROM " + Tables.SCI_STUDENT.name() + " WHERE GPA >= 3.5");
        data.addAll(engineering.select("SELECT * FROM " + Tables.ENG_STUDENT.name() + " WHERE GPA >= 3.5"));
        prettyPrint(data);

        System.out.println("----- ALL STUDENTS WHO ARE ENROLLED IN W2019 -----");
        data = science.select("select s.studentnumber, s.studentname, t.professorname, t.term, e.status " +
                "from " + Tables.SCI_STUDENT.name() + " s join " + Tables.SCI_ENROLLED.name() + " e " +
                "on s.studentnumber = e.studentnumber join " + Tables.SCI_TEACHES.name() + " t " +
                "on t.professorname = e.professorname and t.term = 'W2019'");
        data.addAll(engineering.select("select s.studentnumber, s.studentname, t.professorname, t.term, e.status " +
                "from " + Tables.ENG_STUDENT.name() + " s join " + Tables.ENG_ENROLLED.name() + " e " +
                "on s.studentnumber = e.studentnumber join " + Tables.ENG_TEACHES.name() + " t " +
                "on t.professorname = e.professorname and t.term = 'W2019'"));
        prettyPrint(data);

        System.out.println("----- ALL PROFESSORS AND THEIR RESPECTIVE OFFICE/PHONE NUMBERS -----");
        data = science.select("SELECT * FROM " + Tables.SCI_PROFESSOR.name());
        data2 = engineering.select("SELECT * FROM " + Tables.ENG_PROFESSOR.name());
        for (HashMap<String, String> map : data) {
            for (HashMap<String, String> map2 : data2) {
                if (map2.get("PROFESSORNAME").equals(map.get("PROFESSORNAME"))) {
                    map.put("ENGOFFICE", map2.get("ENGOFFICE"));
                    map.put("ENGPHONE", map2.get("ENGPHONE"));
                    break;
                }
            }
        }
        prettyPrint(data);

        System.out.println("----- HOW MANY COURSES IS EACH STUDENT ENROLLED IN -----");
        data = science.select("select s.studentnumber, s.studentname, count(e.courseNumber) as total_courses " +
                "from " + Tables.SCI_STUDENT.name() + " s join " + Tables.SCI_ENROLLED.name() + " e " +
                "on s.studentnumber = e.studentnumber group by s.studentnumber, s.studentName");
        data.addAll(engineering.select("select s.studentnumber, s.studentname, count(e.courseNumber) as total_courses " +
                "from " + Tables.ENG_STUDENT.name() + " s join " + Tables.ENG_ENROLLED.name() + " e " +
                "on s.studentnumber = e.studentnumber group by s.studentnumber, s.studentName"));
        prettyPrint(data);

        System.out.println("----- PROFESSORS AND THE COURSES THEY'RE TEACHING -----");
        data = science.select("select t.professorname, t.coursenumber from " + Tables.SCI_TEACHES.name() + " t");
        data.addAll(engineering.select("select t.professorname, t.coursenumber from " + Tables.ENG_TEACHES.name() + " t"));
        prettyPrint(data);

        System.out.println("----- HOW MANY STUDENTS ARE ENROLLED IN EACH COURSE -----");
        data = science.select("SELECT e.courseNumber, COUNT(e.CourseNumber) as students_enrolled FROM "
                + Tables.SCI_ENROLLED.name() + " e group by e.courseNumber");
        data.addAll(engineering.select("SELECT e.courseNumber, COUNT(e.CourseNumber) as students_enrolled FROM "
                + Tables.ENG_ENROLLED.name() + " e group by e.courseNumber"));
        prettyPrint(data);

        System.out.println("----- HOW MANY STUDENTS DOES A PROFESSOR TEACH -----");
        data = science.select("SELECT e.professorName, count(e.professorName) as student_count from "
                + Tables.SCI_ENROLLED.name() + " e group by e.professorname");
        data.addAll(engineering.select("SELECT e.professorName, count(e.professorName) as student_count from "
                + Tables.ENG_ENROLLED.name() + " e group by e.professorname"));
        prettyPrint(data);


    }

    public static void prettyPrint(ArrayList<HashMap<String, String>> data) {
        for (int i = 0; i < data.size(); i++) {
            HashMap<String, String> map = data.get(i);
            if (i == 0) {
                System.out.println(String.join(",", map.keySet()));
            }
            System.out.println(String.join(",", map.values()));
        }
    }
}
