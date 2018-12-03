package domain.siteName;

import java.util.List;
import java.util.Scanner;

public class Paypal {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.println("Enter the command");
            System.out.println("(L) -> list users");
            System.out.println("(C) -> create user");
            System.out.println("(F) -> find user by id");
            System.out.println("(+) -> increase user balance");
            System.out.println("(T) -> transaction between 2 users");
            System.out.println("(Q) -> quit");
            String command = scanner.nextLine();
            switch (command) {
                case "L":
                    listUsers();
                    break;
                case "C":
                    System.out.println("First name: ");
                    String firstName = scanner.nextLine();

                    System.out.println("Last name: ");
                    String lastName = scanner.nextLine();

                    System.out.println("Balance: ");
                    double balance = Double.parseDouble(scanner.nextLine());

                    createUser(firstName, lastName, balance);

                    break;
                case "F":
                    System.out.println("User id: ");
                    int userId = Integer.parseInt(scanner.nextLine());

                    findUserById(userId);

                    break;
                case "+":
                    System.out.println("User id: ");
                    int uId = Integer.parseInt(scanner.nextLine());
                    System.out.println("Enter amount: ");
                    double amount = Double.parseDouble(scanner.nextLine());

                    DbHelper.cashFlow(uId, amount);
                    break;
                case "T":
                    System.out.println("UserFrom id: ");
                    int userFromId = Integer.parseInt(scanner.nextLine());
                    System.out.println("UserTo id: ");
                    int userToId = Integer.parseInt(scanner.nextLine());
                    System.out.println("Amount: ");
                    double howMuch = Double.parseDouble(scanner.nextLine());
                    DbHelper.transaction(userFromId, userToId, howMuch);
                    break;
                case "Q":
                    System.out.println("Bye");
                    System.exit(0);
                    break;
                default:
                    System.out.println("Command not found, try again");

            }
        }
    }

    private static void listUsers() {

        List<User> usrs = DbHelper.listUsers();
        for (User user : usrs) {
            System.out.println(user);
        }
    }

    private static void createUser(String firstName, String lastName, double balance) {
        int insertedId = DbHelper.createUser(firstName, lastName, balance);
        System.out.println("Inserted user id is: " + insertedId);
    }

    private static void findUserById(int userId) {
        User userFound = DbHelper.findUserById(userId);
        if (userFound != null)
            System.out.println(userFound);
        else
            System.out.println("There is no user by given id!");
    }
}
