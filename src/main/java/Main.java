import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.*;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Dictionary;
import java.util.Hashtable;

public class Main {
    public static void main(String[] args) {
        System.out.println("Final");

        // Input
        String input = args[0];


        // connect to the pgsql database
        Connection c = null;
        Statement stmt = null;

        // connect to bigquery
        BigQuery bigquery = BigQueryOptions.newBuilder().setProjectId("york-cdf-start")
                .build().getService();

        String insert = "";

        // create the dictionary for holding films
        Dictionary filmdict = new Hashtable();

        try {
            Class.forName("org.postgresql.Driver");
            c = DriverManager
                    .getConnection("jdbc:postgresql://ls-41d379b19b475ed294babb170cfa0f93b3011e47.cq2f1e9koedo.us-east-2.rds.amazonaws.com/dvdrental",
                    "dbmasteruser", "Swnp3XQFtBd)b61NGn!uh{Lw=8#Vk~y<");
            System.out.println("Opened database successfully");

            // write a query
            stmt = c.createStatement();
            ResultSet rs = stmt.executeQuery( "SELECT " +
                    "f.film_id, f.title, cat.name, f.description, f.release_year, f.language_id, " +
                    "f.rental_duration, f.rental_rate, f.length, f.replacement_cost, " +
                    "f.rating, f.last_update, f.special_features, f.fulltext, act.first_name, act.last_name " +
                    "FROM film as f " +
                    "JOIN film_category as fc ON f.film_id = fc.film_id " +
                    "JOIN category as cat ON fc.category_id = cat.category_id " +
                    "JOIN film_actor as fa ON f.film_id = fa.film_id " +
                    "JOIN actor as act ON fa.actor_id = act.actor_id " +
                    "WHERE (act.first_name LIKE '" + input + "' OR act.last_name LIKE '" + input + "') " +
                    "OR (act.first_name LIKE '" + input + "' AND act.last_name LIKE '" + input + "') " +
                    "GROUP BY f.film_id, f.title, cat.name, f.description, f.release_year, f.language_id, " +
                    "f.rental_duration, f.rental_rate, f.length, f.replacement_cost, " +
                    "f.rating, f.last_update, f.special_features, f.fulltext, act.first_name, act.last_name " +
                    "LIMIT 250;" );

            TableRow rowExample = new TableRow();
            rowExample.set("film_id", rs.getInt("film_id"));

            while ( rs.next() ) {
                int film_id = rs.getInt("film_id");
                String title = rs.getString("title").toUpperCase();
                String name = "";
                if (rs.getString("name").substring(0,1).equalsIgnoreCase("D")) {
                     name = rs.getString("name").toUpperCase();
                } else if (rs.getString("name").substring(0,1).equalsIgnoreCase("N")) {
                     name = rs.getString("name").toLowerCase();
                } else {
                     name = rs.getString("name");
                }
                String description = rs.getString("description");
                int release_year = rs.getInt("release_year");
                int language_id = rs.getInt("language_id");
                int rental_duration = rs.getInt("rental_duration");
                double rental_rate = rs.getDouble("rental_rate");
                int length = rs.getInt("length");
                double replacement_cost = rs.getDouble("replacement_cost");
                String rating = rs.getString("rating");
                String last_update = rs.getString("last_update");
                String special_features = rs.getString("special_features");
                String fulltext = rs.getString("fulltext");
                String first_name = rs.getString("first_name");
                String last_name = rs.getString("last_name");

                final String INSERT_FILM =
                        "INSERT INTO `york-cdf-start.final_taylor_bell.final-java` VALUES (" + film_id + ", '" + title + "', '" + name + "', '" + description +
                                "', " + release_year + ", " + language_id + ", " + rental_duration + ", " + rental_rate +
                                ", " + length + ", " + replacement_cost + ", '" + rating + "', '" + last_update + "', '" +
                                special_features + "', \"" + fulltext + "\", '" + first_name + "', '" + last_name + "');";

                // run BigQuery job
                QueryJobConfiguration queryConfig =
                        QueryJobConfiguration.newBuilder(INSERT_FILM).build();

                Job queryJob = bigquery.create(JobInfo.newBuilder(queryConfig).build());
                queryJob = queryJob.waitFor();
                if (queryJob == null) {
                    throw new Exception("job no longer exists");
                }
                // once the job is done, check if any error occured
                if (queryJob.getStatus().getError() != null) {
                    throw new Exception(queryJob.getStatus().getError().toString());
                }

                JobStatistics.QueryStatistics stats = queryJob.getStatistics();
                Long rowsInserted = stats.getDmlStats().getInsertedRowCount();
                System.out.println(title);
                System.out.printf("%d row inserted\n", rowsInserted);
            }

            rs.close();
            stmt.close();
            c.close();

        } catch (Exception e) {
            e.printStackTrace();
            System.err.println(e.getClass().getName() + ": " + e.getMessage());
            System.exit(0);
        }
        System.out.println("Insert Complete");
    }
}
