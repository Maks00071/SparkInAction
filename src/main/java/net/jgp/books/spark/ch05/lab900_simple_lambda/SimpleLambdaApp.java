package net.jgp.books.spark.ch05.lab900_simple_lambda;

import java.util.List;
import java.util.ArrayList;


public class SimpleLambdaApp {

    public static void main(String[] args) {
        List<String> frenchFirstNameList = new ArrayList<>();

        frenchFirstNameList.add("Georges");
        frenchFirstNameList.add("Claude");
        frenchFirstNameList.add("Luc");
        frenchFirstNameList.add("Louis");

        frenchFirstNameList.forEach(
                name -> System.out.println(name + " and Jean-" + name
                        + " are different French first names!"));

        System.out.println("------------------------");

        frenchFirstNameList.forEach(
                name -> {
                    String message = name + " and Jean-";
                    message += name;
                    message += " are different French first names!";
                    System.out.println(message);
                });
    }

    /*
    Georges and Jean-Georges are different French first names!
    Claude and Jean-Claude are different French first names!
    Luc and Jean-Luc are different French first names!
    Louis and Jean-Louis are different French first names!
    ------------------------
    Georges and Jean-Georges are different French first names!
    Claude and Jean-Claude are different French first names!
    Luc and Jean-Luc are different French first names!
    Louis and Jean-Louis are different French first names!
     */
}
