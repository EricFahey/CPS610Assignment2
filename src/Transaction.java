import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Eric Fahey <eric.fahey@cgi.com>
 */
public class Transaction extends Thread {

    private static int count = 1;
    public DatabaseConnection central = new DatabaseConnection(
            "localhost", 1522, "SYSTEM", "admin", ServiceType.SE);
    public DatabaseConnection science = central;
    public DatabaseConnection engineering = new DatabaseConnection(
            "localhost", 1523, "SYSTEM", "admin", ServiceType.XE);
    private int threadId;

    public Transaction() {
        super();
        this.threadId = getNextId();
        central.connect();
        engineering.connect();
    }

    synchronized static int getNextId() {
        return count++;
    }

    public void run() {
        if (threadId == 1) {
            //Truncate Old Data
            truncateTables();

            //Populate Central Database
            populateCentralDatabase();

            //Distribute to Engineering/Science Sites
            populateFacultyDatabases();

            //Reconstruction
            reconstructionQueries();

            //Transactions
            //For Simplicity 'Student' table will be fully replicated
            central.setAutoCommitOff();
            engineering.setAutoCommitOff();

            swapCourse(509, 109, "500634578", "Lilith Hart");
        } else {
            try {
                Thread.sleep(5000);
            } catch (Exception e) {
            }
            //Transactions
            //For Simplicity 'Student' table will be fully replicated
            central.setAutoCommitOff();
            engineering.setAutoCommitOff();

            swapCourse(609, 109, "500645093", "Lilith Hart");
        }


    }

    public void truncateTables() {
        try {
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void populateCentralDatabase() {
        try {
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

            central.insert(Tables.CENTRAL_COURSE, "'109', 'Computer Science 1', 1, 6");
            central.insert(Tables.CENTRAL_COURSE, "'209', 'Computer Science 2', 1, 10");
            central.insert(Tables.CENTRAL_COURSE, "'509', 'Engineering 1', 1, 10");
            central.insert(Tables.CENTRAL_COURSE, "'609', 'Engineering 2', 1, 10");

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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void populateFacultyDatabases() {
        try {
            ArrayList<HashMap<String, String>> data;
            System.out.println("Start Faculty Population!");
            //Students
//        data = central.selectByConditions(Tables.CENTRAL_STUDENT, "Degree='Bachelors of Science'");
            data = central.select(Tables.CENTRAL_STUDENT);
            for (HashMap<String, String> map : data) {
                String attributes = String.join(",", map.keySet());
                String values = String.join(",", map.values());
                science.insert(Tables.SCI_STUDENT, attributes, values);
            }
//        data = central.selectByConditions(Tables.CENTRAL_STUDENT, "Degree='Bachelors of Engineering'");
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
            data = central.selectByAttributesAndConditions(Tables.CENTRAL_COURSE,
                    String.join(",", Tables.SCI_COURSE.getAttributes()), "courseNumber<500");
            for (HashMap<String, String> map : data) {
                String attributes = String.join(",", map.keySet());
                String values = String.join(",", map.values());
                science.insert(Tables.SCI_COURSE, attributes, values);
            }
            data = central.selectByAttributesAndConditions(Tables.CENTRAL_COURSE,
                    String.join(",", Tables.ENG_COURSE.getAttributes()), "courseNumber>=500");
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
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void reconstructionQueries() {
        try {
            ArrayList<HashMap<String, String>> data;
            ArrayList<HashMap<String, String>> data2;
            System.out.println("----- ALL STUDENTS WHO HAVE OVER 3.5 GPA -----");
            data = science.select("SELECT * FROM " + Tables.SCI_STUDENT.name() + " WHERE GPA >= 3.5");
            data.addAll(engineering.select("SELECT * FROM " + Tables.ENG_STUDENT.name() + " WHERE GPA >= 3.5"));
            prettyPrint(data);

            System.out.println("----- ALL STUDENTS NOT ELIGIBLE FOR CO-OP -----");
            data = science.select("SELECT * FROM " + Tables.SCI_STUDENT.name() + " WHERE GPA < 3.0");
            data.addAll(engineering.select("SELECT * FROM " + Tables.ENG_STUDENT.name() + " WHERE GPA < 3.0"));
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

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void swapCourse(int fromCourse, int toCourse, String studentNumber, String toProfessor) {
        System.out.println("----- START OF TRANSACTION " + threadId + " -----");
        ArrayList<HashMap<String, String>> data;
        try {
            science.query("LOCK TABLE sci_enrolled IN EXCLUSIVE MODE");
            engineering.query("LOCK TABLE eng_enrolled IN EXCLUSIVE MODE");
            central.query("LOCK TABLE central_enrolled IN EXCLUSIVE MODE");

            data = central.select("SELECT capacity from central_course WHERE courseNumber = '" + toCourse + "'");
            int capacity = (int) Double.parseDouble((String) data.get(0).values().toArray()[0]);
            data = science.select("SELECT count(studentNumber) as current_capacity from sci_enrolled WHERE courseNumber = '" + toCourse + "'");
            int currentCapacity = (int) Double.parseDouble((String) data.get(0).values().toArray()[0]);
            System.out.println("Current Capacity: " + currentCapacity + "/" + capacity);
            if (currentCapacity >= capacity) {
                System.out.println("ERROR: Course " + toCourse + " already has max capacity!");
                throw new Exception("ERROR: Course " + toCourse + " already has max capacity!");
            }
            //Update Faculty Sites
            engineering.delete(Tables.ENG_ENROLLED, "studentNumber=" + studentNumber + " AND courseNumber=" + fromCourse);
            science.insert(Tables.SCI_ENROLLED, "'" + toCourse + "', '" + toProfessor + "', '" + studentNumber + "', 'Enrolled'");
            //Update Central Site
            central.delete(Tables.CENTRAL_ENROLLED, "studentNumber=" + studentNumber + " AND courseNumber=" + fromCourse);
            central.insert(Tables.CENTRAL_ENROLLED, "'" + toCourse + "', '" + toProfessor + "', '" + studentNumber + "', 'Enrolled'");
            try {
                //Added delay to transaction
                Thread.sleep(15000);
            } catch (Exception e) {
            }
            engineering.getConnection().commit();
            science.getConnection().commit();
        } catch (Exception e) {
            //e.printStackTrace();
            try {
                engineering.getConnection().rollback();
                science.getConnection().rollback();
            } catch (SQLException ee) {
                ee.printStackTrace();
                System.out.println("Failed to rollback transactions!");
            }
        } finally {
            System.out.println("------ CLASS LIST FOR TRANSACTION " + threadId + " -------");
            try {
                data = science.select("select s.studentName, s.studentNumber, e.courseNumber, e.professorName, " +
                        "e.status, s.degree from sci_enrolled e join sci_student s on s.studentNumber = e.studentNumber " +
                        "where e.courseNumber = '109'");
                prettyPrint(data);
            } catch (SQLException e) {
                e.printStackTrace();
                System.out.println("Failed to print class list!");
            }
        }
        System.out.println("----- END OF TRANSACTION " + threadId + " -----");
    }

    public void prettyPrint(ArrayList<HashMap<String, String>> data) {
        for (int i = 0; i < data.size(); i++) {
            HashMap<String, String> map = data.get(i);
            if (i == 0) {
                System.out.println(String.join(",", map.keySet()));
            }
            System.out.println(String.join(",", map.values()));
        }
    }
}
