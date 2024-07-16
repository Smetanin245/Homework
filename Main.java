import org.postgresql.Driver;
import java.sql.*;

public class Main {
    public static void main(String[] args) {
        try {
            Class.forName("org.postgresql.Driver");
            System.out.println("Connected JDBC Driver");
            System.out.println(" ");
            Connection connection = DriverManager.getConnection(
                    "jdbc:postgresql://localhost/galery",
                    "postgres",
                    "postgres");


            Statement statement = connection.createStatement();

            statement.executeQuery("SELECT * FROM works;");
            //System.out.println(statement.executeQuery("SELECT * FROM works;"));

            ResultSet resultSet = statement.executeQuery(
                    "SELECT name FROM works;");


            getArtistsByCountry(connection,"Испания");
            System.out.println(" ");
            getPaintingsByArtist(connection,1);
            System.out.println(" ");
            getPaintingsByprice(connection,0);
            System.out.println(" ");
            uddPainting(connection,"Портрет А. С. Пушкина","1827",
                    "масло","Романтизм",0,0,1);
            System.out.println(" ");
            removePainting(connection,"Портрет А. С. Пушкина",3);
            System.out.println(" ");
            correctPaintingsPrice(connection,"Подсолнухи",100000);
            System.out.println(" ");
            getPaintingsAdress(connection,1);
            System.out.println(" ");
            getPaintingsByStyle(connection,"романтизм");
            System.out.println(" ");
            removeMuseum(connection,"Третьяковская галерея",1);
            System.out.println(" ");
            uddMuseum(connection,"Третьяковская галерея","Россия","Лаврушинский пер., 10");


            statement.close();
            connection.close();

        } catch (ClassNotFoundException e) {
            throw new RuntimeException("JDBC Driver is not found. Include it in your library path");

        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private static void getArtistsByCountry(Connection connection , String country) throws SQLException{
        if (country == null || country.isBlank()) return;


        String columnName0 = "id", columnName1 = "fullname", columnName2 = "years_of_living",
         columnName3 = "country", columnName4 = "style";

        int param0 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM artists WHERE country ='"+country+"';");

        while (rs.next()) {
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + "|" + param3 + " | " + param4);
        }
    }

    private static void getPaintingsByArtist(Connection connection , int id) throws SQLException{
        if (id <= 0) return;

        String columnName0 = "id", columnName1 = "name", columnName2 = "year",
                columnName3 = "material", columnName4 = "style", columnName5 = "price",
                columnName6 = "author_id", columnName7 = "museum_id";

        int param0 = -1, param5 = -1, param6 = -1, param7 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM works WHERE author_id ="+id+";"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {
            param7 = rs.getInt(columnName7);
            param6 = rs.getInt(columnName6);
            param5 = rs.getInt(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + "|" + param3 + " | " + param4 + " | " + param5
                    + " | " + param6 + " | " + param7);
        }
    }


    private static void getPaintingsByprice(Connection connection , int price) throws SQLException{
        if (price < 0) return;

        String columnName0 = "id", columnName1 = "name", columnName2 = "year",
                columnName3 = "material", columnName4 = "style", columnName5 = "price",
                columnName6 = "author_id", columnName7 = "museum_id";

        int param0 = -1, param5 = -1, param6 = -1, param7 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT * FROM works WHERE price ="+price+";"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {
            param7 = rs.getInt(columnName7);
            param6 = rs.getInt(columnName6);
            param5 = rs.getInt(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + "|" + param3 + " | " + param4 + " | " + param5
                    + " | " + param6 + " | " + param7);
        }
    }

    private static void uddPainting(Connection connection ,String name, String yearOfCreation,String material,
                                    String style, int price, int author_id, int museum_id) throws SQLException{
        if (name == null || name.isBlank() || yearOfCreation == null || yearOfCreation.isBlank()
                || material == null || material.isBlank() || style == null || style.isBlank() ||
                price < 0 || author_id <= 0 || museum_id<=0) return;


        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO works(name, year,material,style,price,author_id,museum_id) VALUES (?, ?, ?, " +
                        "?, ?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);
        statement.setString(2, yearOfCreation);
        statement.setString(3, material);
        statement.setString(4, style);
        statement.setInt(5, price);
        statement.setInt(6, author_id);
        statement.setInt(7, museum_id);


        int count = statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор картины " + rs.getInt(1));
        }

        PreparedStatement statement_two = connection.prepareStatement("insert into public.museum_and_works(museum_id, works_id)values (?,?);");
        statement_two.setInt(1, museum_id);
        statement_two.setInt(2, rs.getInt(1));

        int count_two = statement.executeUpdate();


        System.out.println("Inserted " + count + " painting");

        }
    private static void removePainting(Connection connection, String name,int painting_id) throws SQLException {
        if (name == null || name.isBlank() || painting_id<=0) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from works WHERE name=?;");
        statement.setString(1, name);

        int count = statement.executeUpdate();

        PreparedStatement statement_two = connection.prepareStatement("DELETE from museum_and_works WHERE works_id=?;");
        statement_two.setInt(1, painting_id);

        int count_two = statement_two.executeUpdate();
        System.out.println("Deleted " + count + " paintings");

    }

    private static void correctPaintingsPrice (Connection connection, String name, int price) throws SQLException {
        if (name == null || name.isBlank() || price < 0) return;

        PreparedStatement statement = connection.prepareStatement("UPDATE works SET price=? WHERE name=?;");
        statement.setInt(1, price);
        statement.setString(2, name);

        int count = statement.executeUpdate();

        System.out.println("Updated " + count + " works");

    }
    private static void getPaintingsAdress(Connection connection , int id) throws SQLException{
        if (id <= 0) return;

        String columnName0 = "address";

        String param0 = null;

        Statement statement = connection.createStatement();
        ResultSet rs = statement.executeQuery("SELECT m.address \n" +
                "FROM museum as m inner join museum_and_works as mw on " +
                "mw.museum_id = m.id inner join works as w on mw.works_id = w.id\n" +
                "where w.id = "+id+";");
        while (rs.next()) {
            param0 = rs.getString(columnName0);
            System.out.println(param0);
        }
    }

    private static void getPaintingsByStyle(Connection connection , String style) throws SQLException{
        if (style == null || style.isBlank()) return;

        String columnName0 = "id", columnName1 = "name", columnName2 = "year",
                columnName3 = "material", columnName4 = "style", columnName5 = "price",
                columnName6 = "author_id", columnName7 = "museum_id";

        int param0 = -1, param5 = -1, param6 = -1, param7 = -1;
        String param1 = null, param2 = null, param3 = null, param4 = null;

        Statement statement = connection.createStatement();     // создаем оператор для простого запроса (без параметров)
        ResultSet rs = statement.executeQuery("SELECT * FROM works WHERE lower(style) =lower('"+style+"');"); // выполняем запроса на поиск и получаем список ответов

        while (rs.next()) {
            param7 = rs.getInt(columnName7);
            param6 = rs.getInt(columnName6);
            param5 = rs.getInt(columnName5);
            param4 = rs.getString(columnName4);
            param3 = rs.getString(columnName3);
            param2 = rs.getString(columnName2);
            param1 = rs.getString(columnName1);
            param0 = rs.getInt(columnName0);
            System.out.println(param0 + " | " + param1 + " | " + param2 + "|" + param3 + " | " + param4 + " | " + param5
                    + " | " + param6 + " | " + param7);
        }
    }
    private static void removeMuseum(Connection connection, String name,int museum_id) throws SQLException {
        if (name == null || name.isBlank()) return;

        PreparedStatement statement = connection.prepareStatement("DELETE from museum WHERE name=?;");
        statement.setString(1, name);

        int count = statement.executeUpdate();

        PreparedStatement statement_two = connection.prepareStatement("DELETE from museum_and_works WHERE museum_id=?;");
        statement_two.setInt(1, museum_id);

        int count_two = statement_two.executeUpdate();
        System.out.println("Deleted " + count + " museum");

    }
    private static void uddMuseum(Connection connection ,String name, String country,String address) throws SQLException{
        if (name == null || name.isBlank() || country == null || country.isBlank()
                || address == null || address.isBlank()) return;


        PreparedStatement statement = connection.prepareStatement(
                "INSERT INTO museum(name, country,address) VALUES (?, ?, ?) returning id;", Statement.RETURN_GENERATED_KEYS);    // создаем оператор шаблонного-запроса с "включаемыми" параметрами - ?
        statement.setString(1, name);
        statement.setString(2, country);
        statement.setString(3, address);


        int count = statement.executeUpdate();
        ResultSet rs = statement.getGeneratedKeys();
        if (rs.next()) {
            System.out.println("Идентификатор музея " + rs.getInt(1));
        }

        System.out.println("Inserted " + count + " museum");

    }

    }

