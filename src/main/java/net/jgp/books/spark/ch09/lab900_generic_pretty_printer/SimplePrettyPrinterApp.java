package net.jgp.books.spark.ch09.lab900_generic_pretty_printer;

public class SimplePrettyPrinterApp {

    public static void main(String[] args) {
        SimplePrettyPrinterApp app = new SimplePrettyPrinterApp();
        app.start();
    }

    /**
     * Start the application
     */
    private void start() {
        // create a book
        Book book = new Book();
        book.setTitle("Spark with Java");
        book.setAuthor("Jean Georges Perrin");
        book.setIsbn("9781617295522");
        book.setPublicationYear(2019);
        book.setUrl("https://www.manning.com/books/spark-with-java");

        // create an author
        Author author = new Author();
        author.setName("Jean Georges Perrin");
        author.setDob("1971-10-05");
        author.setUrl("https://en.wikipedia.org/wiki/Jean_Georges_Perrin");

        // dumps the result
        System.out.println("A book...");
        PrettyPrinterUtils.print(book);
        System.out.println("An author...");
        PrettyPrinterUtils.print(author);
    }

    /*
    A book...
    PublicationYear: 2019
    Title: Spark with Java
    Author: Jean Georges Perrin
    Isbn: 9781617295522
    Url: https://www.manning.com/books/spark-with-java
    An author...
    Name: Jean Georges Perrin
    Url: https://en.wikipedia.org/wiki/Jean_Georges_Perrin
    Dob: 1971-10-05
     */
}
