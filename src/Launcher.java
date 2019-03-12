import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @author Eric Fahey <eric.fahey@ryerson.ca>
 */
public class Launcher {

    public static final int THREADS = 2;

    public static ArrayList<Transaction> transactions = new ArrayList<>();

    public static void main(String[] args) {
        try {
            //Create Transactions and create DB connections
            for (int i = 0; i < THREADS; i++) {
                Transaction transaction = new Transaction();
                transactions.add(transaction);
            }

            //Start the threads
            for (Transaction transaction : transactions) {
                transaction.start();
            }

            //Exit the threads
            for (Transaction transaction : transactions) {
                transaction.join();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
