import java.io.*;
import java.sql.*;
import java.util.HashMap;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

/***
 * @author : Jahongir Amirkulov
 * @author : Nishi Parameshwara
 * @author : Sharwari Salunkhe
 *
 */

/**
 * This class loads data from 2 different datasets into one
 * single database with multiple tables
 */
public class DataLoading {

    static Connection conn = null;
    static PreparedStatement ps = null;
    static Statement stmt = null;
    static long startTime, duration;
    static String url = "jdbc:mysql://localhost:3306?serverTimezone=UTC&allowLoadLocalInfile=true";
    static String user = "root";
    static String pwd = "password";

    public static void main(String[] args) throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        conn = DriverManager.getConnection(url, user, pwd);
        stmt = conn.createStatement();
        System.out.println("Connected to the database");
        stmt = conn.createStatement();
        // comment the next 2 line once you run it for the first time
        String sql = "CREATE DATABASE if not exists recommend";
        stmt.executeUpdate(sql);
        System.out.println("Database created successfully...");
        createTables();
        loadmovieInfo();
        loadratinglens();
        loadlenstags();
        loadlenslinks();
        insertPersonTable();
        insertMovieTable();
        insertRatings();
//        actorMovieRole_table();
//        role_table();
//        written_table();
//        directed_table();
//        movieActor_table();
    }

    /**
     * Method to create all tables from the schema
     *
     * @throws SQLException
     */
    public static void createTables() throws SQLException {
        stmt = conn.createStatement();
        // change dbName if you want
        stmt.executeUpdate("USE recommend");
        System.out.println("Using Database");
        String movieTable = "CREATE TABLE IF NOT EXISTS movielenstable(\n" +
                "movieId INTEGER not null,\n " +
                " title VARCHAR(200) not null,\n" +
                " genres VARCHAR(100) not null,\n" +
                " PRIMARY KEY(movieId))\n";
        ps = conn.prepareStatement(movieTable);
        ps.executeUpdate(movieTable);

        // table 2
        String ratingsTable = "create table if not exists lensratings(" +
                "userId INTEGER not null,\n" +
                "movieId INTEGER not null,\n" +
                "rating INTEGER not null,\n" +
                "FOREIGN KEY (movieId) REFERENCES movielenstable(movieId) )";

        ps = conn.prepareStatement(ratingsTable);
        ps.executeUpdate(ratingsTable);

        //table 3
        String tagsTable = "create table if not exists lenstags(\n" +
                "userId int not null,\n" +
                "movieId int not null,\n" +
                "tag VARCHAR(1000) not null,\n" +
                "FOREIGN KEY (movieId) REFERENCES movielenstable(movieId))";
        ps = conn.prepareStatement(tagsTable);
        ps.executeUpdate(tagsTable);

        String personTable = "CREATE TABLE IF NOT EXISTS Person" +
                "( personID INTEGER not null, " +
                " personName VARCHAR(200)," +
                " PRIMARY KEY(personID) )";
        ps = conn.prepareStatement(personTable);
        ps.executeUpdate(personTable);

        String imdbMovieTable = "CREATE TABLE IF NOT EXISTS ImdbMovie " +
                "( imdbId INTEGER not null," +
                " title VARCHAR(1000) not null, " +
                " releaseYear INTEGER(4) null, " +
                " runtime INTEGER , " +
                " rating FLOAT , " +
                " numberOfVotes INTEGER , " +
                "PRIMARY KEY (imdbId) )";
        ps = conn.prepareStatement(imdbMovieTable);
        ps.executeUpdate(imdbMovieTable);

        String linksTable = "create table if not exists lenslinks(\n" +
                "movieId int not null,\n" +
                "imdbId int not null,\n" +
                "FOREIGN KEY (movieId) REFERENCES movielenstable(movieId),\n" +
                "FOREIGN KEY (imdbId) REFERENCES ImdbMovie(imdbId))\n";
        ps = conn.prepareStatement(linksTable);
        ps.executeUpdate(linksTable);

        // Closing Connection
        ps.close();
        conn.close();

    }

    /**
     * Method to insert data in the movies table
     *from the MovieLens dataset
     * @throws Exception
     */
    public static void loadmovieInfo() throws Exception {
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        // conn.setAutoCommit(false);
        stmt.executeUpdate("USE recommend");
        System.out.println("Loading data into table movielenstable");
        String query = " LOAD DATA local INFILE '" + "/Users/jahongiramirkulov/Desktop/RIT/classes/fall/big_data_analytics/recommendation-system/recommendation-system/data/movies.csv" +
                "' INTO TABLE movielenstable\n" +
                "FIELDS TERMINATED BY ','\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 LINES\n" +
                "(@movieId,@title,@genres)\n" +
                "SET movieId = @movieId, title = @title, genres = @genres;";
        stmt.executeQuery(query);
        System.out.println("Inserted data from movies.csv");
        stmt.close();
        conn.close();
    }

    /**
     * Method to load ratings per user for given movies
     * from the MovieLens dataset
     * @throws Exception
     */
    public static void loadratinglens() throws Exception {
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        // conn.setAutoCommit(false);
        stmt.executeUpdate("USE recommend");
        System.out.println("Loading data into table lensratings");

        startTime = System.nanoTime();
        // query to insert data into table
        String query = " LOAD DATA local INFILE '" + "/Users/jahongiramirkulov/Desktop/RIT/classes/fall/big_data_analytics/recommendation-system/recommendation-system/data/ratings.csv" +
                "' INTO TABLE lensratings\n" +
                "FIELDS TERMINATED BY ','\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 LINES\n" +
                "(@userId,@movieId,@rating,@timestamp)\n" +
                "SET userId = @userId, movieId = @movieId, rating = @rating;";
        stmt.executeQuery(query);
        System.out.println("Inserted data from ratings.csv");
//        duration = System.nanoTime() - startTime;
//        System.out.println("Time taken for loading data in vehicleInfo table: " + duration);
        stmt.close();
        conn.close();
    }

    /**
     * Method to load tags for each movie given by each user
     * e.g funny, documentary etc
     * @throws Exception
     */
    public static void loadlenstags() throws Exception {
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        // conn.setAutoCommit(false);
        stmt.executeUpdate("USE recommend");
        System.out.println("Loading data into table lenstags");

        startTime = System.nanoTime();
        // query to insert data into table
        String query = " LOAD DATA local INFILE '" + "/Users/jahongiramirkulov/Desktop/RIT/classes/fall/big_data_analytics/recommendation-system/recommendation-system/data/ref_tags.csv" +
                "' INTO TABLE lenstags\n" +
                "FIELDS TERMINATED BY ','\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 LINES\n" +
                "(@userId,@movieId,@tag,@timestamp)\n" +
                "SET userId = @userId, movieId = @movieId, tag = @tag;";
        stmt.executeQuery(query);
        System.out.println("Inserted data from tags.csv");
        stmt.close();
        conn.close();
    }

    /**
     * Method to load the links.csv file. This file has movieId from the movieLens dataset
     * and its corresponding ImdbId that we can correlate
     * @throws Exception
     */
    public static void loadlenslinks() throws Exception {
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE recommend");
        System.out.println("Loading data into table lenslinks");

        startTime = System.nanoTime();
        // query to insert data into table
        String query = " LOAD DATA local INFILE '" + "/Users/jahongiramirkulov/Desktop/RIT/classes/fall/big_data_analytics/recommendation-system/recommendation-system/data/links.csv" +
                "' INTO TABLE lenslinks\n" +
                "FIELDS TERMINATED BY ','\n" +
                "LINES TERMINATED BY '\\n'\n" +
                "IGNORE 1 LINES\n" +
                "(@movieId,@imdbId,@tmdbId)\n" +
                "SET movieId = @movieId, imdbId = @imdbId;";
        stmt.executeQuery(query);
        System.out.println("Inserted data from links.csv");
        stmt.close();
        conn.close();
    }

    /**
     * Method to load people from the imdb dataset
     * each person is either actor, director, writer and so on
     * identified by the personId
     * @throws Exception
     */
    public static void insertPersonTable() throws Exception {
        InputStream gzip = new GZIPInputStream(new FileInputStream("name.basics.tsv.gz"));
        Scanner scanner1 = new Scanner(gzip, "UTF-8");
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE recommend");
        conn.setAutoCommit(false);
        String tryquery = "INSERT INTO Person " +
                "(personID, personName)" +
                " VALUES (?, ?)";
        PreparedStatement ps = conn.prepareStatement(tryquery);
        scanner1.nextLine();
        int count = 1;
        int totalcount = 0;
        while (scanner1.hasNextLine()) {
            String[] arrofStr = scanner1.nextLine().split("\\t");
            ps.setInt(1, Integer.parseInt(arrofStr[0].replace("nm", "")));
            ps.setString(2, arrofStr[1]);

            count++;
            ps.addBatch();
            if (count % 1000000 == 0) {
                ps.executeBatch();
                conn.commit();
                count = 0;
            }
            //    }
        }
        if (count > 0) {
            ps.executeBatch();
            conn.commit();
        }
        ps.close();
        stmt.close();
        conn.close();
    }

    /**
     * This method loads all the movies from imdb dataset
     * @throws Exception
     */
    public static void insertMovieTable() throws Exception {

        InputStream gzip = new GZIPInputStream(new FileInputStream("title.basics.tsv.gz"));
        Scanner scanner = new Scanner(gzip, "UTF-8");
        Connection conn = DriverManager.getConnection(url, user, pwd);
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("USE recommend");
        conn.setAutoCommit(false);

        String tryquery = "INSERT IGNORE INTO ImdbMovie(imdbID, title, releaseYear, runtime)" +
                " VALUES (?, ?, ?, ?)";

        PreparedStatement ps = conn.prepareStatement(tryquery);

        // ignores first line of input file
        scanner.nextLine();
        int count = 0;
        while (scanner.hasNextLine()) {

            String[] arrofStr = scanner.nextLine().split("\\t");
            if (arrofStr[1].equals("movie") || arrofStr[1].equals("tvMovie")) {
                ps.setInt(1, Integer.parseInt(arrofStr[0].replace("tt", "")));
                ps.setString(2, arrofStr[3]);
                if (!arrofStr[5].contains("N")) {
                    ps.setInt(3, Integer.parseInt(arrofStr[5]));
                }
                if (!arrofStr[7].contains("N")) {
                    ps.setInt(4, Integer.parseInt(arrofStr[7]));
                }
                count++;
                ps.addBatch();
                if (count % 1000000 == 0) {
                    ps.executeBatch();
                    conn.commit();
                    count = 0;
                }
            }
            if (count > 0) {
                ps.executeBatch();
                conn.commit();
            }


        }
        ps.close();
        stmt.close();
        conn.close();
        scanner.close();
    }

       /**
       * This method loads the ratings of all movies from the imdb dataset
       * @throws Exception
       */
        public static void insertRatings () throws Exception {
            InputStream gzip1 = new GZIPInputStream(new FileInputStream("title.ratings.tsv.gz"));
            Scanner scanner1 = new Scanner(gzip1, "UTF-8");
            Connection conn = DriverManager.getConnection(url, user, pwd);
            Statement stmt = conn.createStatement();
            stmt.executeUpdate("USE recommend");
            conn.setAutoCommit(false);
            String tryquery1 = "UPDATE ImdbMovie SET rating = ?, numberOfVotes = ? WHERE imdbID = ? ";

            PreparedStatement ps = conn.prepareStatement(tryquery1);
            System.out.println("Entered ratings file");
            // ignores first line of input file
            scanner1.nextLine();
            int count = 0;
            while (scanner1.hasNextLine()) {
                String[] arrofStr1 = scanner1.nextLine().split("\\t");
                ps.setInt(3, Integer.parseInt(arrofStr1[0].replace("tt", "")));
                ps.setFloat(1, Float.parseFloat(arrofStr1[1]));
                ps.setInt(2, Integer.parseInt(arrofStr1[2]));
                count++;
                ps.addBatch();
                if (count % 1000000 == 0) {
                    ps.executeBatch();
                    conn.commit();
                    count = 0;
                }

            }
            if (count > 0) {
                ps.executeBatch();
                conn.commit();
            }
            scanner1.close();
            ps.close();
            conn.close();
        }

    /**
     * This method adds every actor with the movie he/she worked in
     */
    public static void movieActor_table () {
            PreparedStatement acted_table;
            PreparedStatement acted_insert;
            int header = 0;
            String[] val;
            String value;
            String value2;
            String value3;
            String pattern = "(?<=^..)(.*)";

            try {
                conn = DriverManager.getConnection(url, user, pwd);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("USE recommend");
                acted_table = conn.prepareStatement("CREATE TABLE IF NOT EXISTS movie_actor(actor int NOT NULL, " +
                        "imdbMovie int NOT NULL, PRIMARY KEY (actor,imdbMovie), FOREIGN KEY(actor) REFERENCES member(id)," +
                        "FOREIGN KEY(imdbMovie) REFERENCES imdbMovie(imdbId))");
                acted_table.execute();
                System.out.println("Creating Movie_Actor Table");

                acted_insert = conn.prepareStatement("INSERT INTO movie_actor(actor,imdbMovie) SELECT " +
                        "person.personID, imdbMovie.imdbId from person,imdbMovie where person.personID = ? and imdbMovie.imdbId = ? ON CONFLICT " +
                        "(actor,imdbMovie) DO NOTHING");

                InputStream principals_file = new GZIPInputStream(new FileInputStream("title.principals.tsv.gz"));
                BufferedReader title_principals_read = new BufferedReader(new InputStreamReader(principals_file));
                System.out.println("Reading title.principals.tsv.gz file");

                conn.setAutoCommit(false); // default true

                while (title_principals_read.readLine() != null) {
                    String line = title_principals_read.readLine();

                    if (header == 0) {
                        header++;
                        continue;
                    }

                    if (line == null) {
                        continue;
                    }

                    val = line.split("\t");
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(val[0]);
                    Matcher n = r.matcher(val[2]);
                    value2 = val[3];

                    if (value2.equals("actor") | value2.equals("actress")) {
                        if (m.find() && n.find()) {
                            value = m.group(1);
                            acted_insert.setInt(1, Integer.parseInt(value));
                            value3 = n.group(1);
                            acted_insert.setInt(2, Integer.parseInt(value3));
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    acted_insert.addBatch();
                    if (++header % 100000 == 0) {
                        acted_insert.executeBatch();
                    }
                }
                acted_insert.executeBatch();
                conn.commit();
                System.out.println("Acted Table Inserted");
            } catch (SQLException | NullPointerException | IOException e) {
                e.printStackTrace();
            }
        }

    /**
     * This method loads data for every director who has
     * directed respective movies
     */
    public static void directed_table () {
            PreparedStatement director;
            PreparedStatement directed_insert;
            int header = 0;
            String[] val;
            String value2;
            String value3;
            String pattern = "(?<=^..)(.*)";

            try {
                conn = DriverManager.getConnection(url, user, pwd);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("USE recommend");
                director = conn.prepareStatement("CREATE TABLE IF NOT EXISTS movie_director(director int " +
                        "NOT NULL, imdbId int NOT NULL, PRIMARY KEY(director,imdbId), FOREIGN KEY (director) REFERENCES " +
                        "member(id), FOREIGN KEY (movie) REFERENCES imdbMovie(imdbId))");
                director.execute();
                System.out.println("Creating Movie_Director Table");

                directed_insert = conn.prepareStatement("INSERT INTO movie_director(director,imdbId) SELECT " +
                        "person.personID, imdbMovie.imdbId from person,imdbMovie where person.personID = ? and imdbMovie.imdbId = ? ON CONFLICT " +
                        "(director,imdbMovie) DO NOTHING");

                InputStream principals_file = new GZIPInputStream(new FileInputStream(
                        "title.principals.tsv.gz"));
                BufferedReader title_principals_read = new BufferedReader(new InputStreamReader(principals_file));
                System.out.println("Reading title.principals.tsv.gz file");
                conn.setAutoCommit(false); // default true
                while (title_principals_read.readLine() != null) {
                    String line = title_principals_read.readLine();
                    //System.out.println(line);
                    if (header == 0) {
                        header++;
                        continue;
                    }
                    if (line == null) {
                        continue;
                    }
                    val = line.split("\t");
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(val[0]);
                    Matcher n = r.matcher(val[2]);
                    value2 = val[3];

                    if (value2.equals("director")) {
                        if (m.find() && n.find()) {
                            value2 = m.group(1);
                            directed_insert.setInt(1, Integer.parseInt(value2));
                            value3 = n.group(1);
                            directed_insert.setInt(2, Integer.parseInt(value3));
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    directed_insert.addBatch();
                    if (++header % 100000 == 0) {
                        directed_insert.executeBatch();
                    }
                }
                directed_insert.executeBatch();
                conn.commit();
                System.out.println("Directed Table Inserted");
            } catch (SQLException | NullPointerException | IOException e) {
                e.printStackTrace();
            }
        }

    /**
     * This method loads data for every writer who has
     * written respective movies
     */
        public static void written_table () {
            PreparedStatement writer;
            PreparedStatement written_insert;
            int header = 0;
            String[] val;
            String value2;
            String value3;
            String pattern = "(?<=^..)(.*)";

            try {
                conn = DriverManager.getConnection(url, user, pwd);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("USE recommend");
                writer = conn.prepareStatement("CREATE TABLE IF NOT EXISTS movie_writer(writer int NOT NULL,"
                        + " imdbMovie int NOT NULL, PRIMARY KEY(writer,imdbMovie), FOREIGN KEY (writer) REFERENCES person(personID)" +
                        ", FOREIGN KEY (imdbMovie) REFERENCES imdbMovie(imdbId))");
                writer.execute();
                System.out.println("Creating Movie_Writer Table");

                written_insert = conn.prepareStatement("INSERT INTO movie_writer(writer,ImdbMovie) SELECT " +
                        "person.personID, imdbMovie.imdbId from person,ImdbMovie where person.personID = ? and ImdbMovie.imdbId = ? ON CONFLICT " +
                        "(writer,imdbMovie) DO NOTHING");

                InputStream principals_file = new GZIPInputStream(new FileInputStream("title.principals.tsv.gz"));
                BufferedReader title_principals_read = new BufferedReader(new InputStreamReader(principals_file));
                System.out.println("Reading title.principals.tsv.gz file");
                conn.setAutoCommit(false); // default true
                while (title_principals_read.readLine() != null) {
                    String line = title_principals_read.readLine();
                    //System.out.println(line);
                    if (header == 0) {
                        header++;
                        continue;
                    }
                    if (line == null) {
                        continue;
                    }
                    val = line.split("\t");
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(val[0]);
                    Matcher n = r.matcher(val[2]);
                    value2 = val[3];

                    if (value2.equals("writer")) {
                        if (m.find() && n.find()) {
                            value2 = m.group(1);
                            written_insert.setInt(1, Integer.parseInt(value2));
                            value3 = n.group(1);
                            written_insert.setInt(2, Integer.parseInt(value3));
                        } else {
                            continue;
                        }
                    } else {
                        continue;
                    }

                    written_insert.addBatch();
                    if (++header % 100000 == 0) {
                        written_insert.executeBatch();
                    }
                }
                written_insert.executeBatch();
                conn.commit();
                System.out.println("Written Table Inserted");
            } catch (SQLException | NullPointerException | IOException e) {
                e.printStackTrace();
            }
        }

    /**
     * This method loads data for every unique role
     * that every person has worked on
     */
        public static void role_table () {
            HashMap<String, Integer> dict1 = new HashMap<String, Integer>();
            PreparedStatement member_roles;
            PreparedStatement role_insert;

            int id = 1;
            int header = 0;
            String[] val;

            try {
                conn = DriverManager.getConnection(url, user, pwd);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("USE recommend");
                member_roles = conn.prepareStatement("CREATE TABLE IF NOT EXISTS role(id " +
                        "INT PRIMARY KEY NOT NULL, role TEXT)");
                member_roles.execute();
                System.out.println("Creating Role Table");

                role_insert = conn.prepareStatement("INSERT INTO role(id, role)" +
                        " VALUES (?, ?)");

                InputStream principals_file = new GZIPInputStream(new FileInputStream("title.principals.tsv.gz"));
                BufferedReader title_principals_read = new BufferedReader(new InputStreamReader(principals_file));
                System.out.println("Reading title.principals.tsv.gz file");

                conn.setAutoCommit(false); // default true
                while (title_principals_read.readLine() != null) {
                    String line = title_principals_read.readLine();
                    if (header == 0) {
                        header++;
                        continue;
                    }
                    if (line == null) {
                        continue;
                    }
                    val = line.split("\t");
                    if (val[5].equals("\\N")) {
                        continue;
                    } else {
                        String role = val[5];
                        if (!dict1.containsKey(role)) {
                            dict1.put(role, id);
                            role_insert.setInt(1, id);
                            role_insert.setString(2, role);
                            id++;
                            role_insert.execute();
                        }
                    }
                }

                conn.commit();
                System.out.println("Role Table Inserted");
            } catch (SQLException | NullPointerException | IOException e) {
                e.printStackTrace();
            }
        }

    /**
     *
     * @throws SQLException
     */
    public static void actorMovieRole_table () throws SQLException {
            PreparedStatement movie_acted_role_table;
            PreparedStatement movie_acted_role_insert;
            int header = 0;
            String[] val;
            String value;
            String value3;
            String pattern = "(?<=^..)(.*)";

            try {
                conn = DriverManager.getConnection(url, user, pwd);
                Statement stmt = conn.createStatement();
                stmt.executeUpdate("USE recommend");
                movie_acted_role_table = conn.prepareStatement("CREATE TABLE IF NOT EXISTS movie_actor_role(actor "
                        + "int NOT NULL, imdbMovie int NOT NULL, role int NOT NULL, PRIMARY KEY (actor,movie,role), FOREIGN " +
                        "KEY (actor) REFERENCES person(personID), FOREIGN KEY(movie) REFERENCES imdbMovie(imdbId), FOREIGN KEY(role) " +
                        "REFERENCES role(id))");
                movie_acted_role_table.execute();
                System.out.println("Creating Movie_Actor_Role Table");

                movie_acted_role_insert = conn.prepareStatement("INSERT INTO movie_actor_role(actor,movie,role) " +
                        "SELECT movie_actor.actor, movie_actor.movie, role.id from movie_actor,role where " +
                        "movie_actor.actor = ? and movie_actor.movie = ? and role.role = ? ON CONFLICT (actor,movie,role)" +
                        " DO NOTHING");

                InputStream principals_file = new GZIPInputStream(new FileInputStream("title.principals.tsv.gz"));
                BufferedReader title_principals_read = new BufferedReader(new InputStreamReader(principals_file));
                System.out.println("Reading title.principals.tsv.gz file");
                conn.setAutoCommit(false); // default true
                while (title_principals_read.readLine() != null) {
                    String line = title_principals_read.readLine();
                    if (header == 0) {
                        header++;
                        continue;
                    }
                    if (line == null) {
                        continue;
                    }
                    val = line.split("\t");
                    Pattern r = Pattern.compile(pattern);
                    Matcher m = r.matcher(val[0]);
                    Matcher n = r.matcher(val[2]);
                    if (val[3].equals("actor") | val[3].equals("actress")) {
                        if (m.find() && n.find() && !val[5].equals("\\N")) {
                            value = m.group(1);
                            movie_acted_role_insert.setInt(1, Integer.parseInt(value));
                            value3 = n.group(1);
                            movie_acted_role_insert.setInt(2, Integer.parseInt(value3));
                            movie_acted_role_insert.setString(3, val[5]);
                        } else {
                            continue;
                        }
                    }
                    movie_acted_role_insert.addBatch();
                    if (++header % 100000 == 0) {
                        movie_acted_role_insert.executeBatch();
                    }
                }
                movie_acted_role_insert.executeBatch();
                conn.commit();
                System.out.println("Acted Table Inserted");
            } catch (SQLException | NullPointerException | IOException e) {
                e.printStackTrace();
            }
        }
    }
