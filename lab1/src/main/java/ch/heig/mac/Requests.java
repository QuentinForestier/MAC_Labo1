package ch.heig.mac;

import java.util.List;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;


public class Requests {
    private final Cluster cluster;

    public Requests(Cluster cluster) {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames() {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating() {
        QueryResult result = cluster.query(
                "SELECT imdb.id imdb_id, tomatoes.viewer.rating " +
                        "tomato_rating, imdb.rating imdb_rating\n" +
                        "FROM `mflix-sample`._default.movies\n" +
                        "WHERE tomatoes.viewer.rating != 0 AND ABS(tomatoes" +
                        ".viewer.rating - imdb.rating) > 7"
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<String> greatReviewers() {
        QueryResult result = cluster.query(
                "SELECT RAW name \n" +
                         "FROM `mflix-sample`.`_default`.`comments` \n" +
                         "GROUP BY name \n" +
                         "HAVING COUNT(_id) > 300;"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor) {
        QueryResult result = cluster.query(
                "SELECT imdb.id, imdb.rating, `cast` \n" +
                         "FROM `mflix-sample`.`_default`.`movies` \n" +
                         "WHERE \"" + actor + "\" IN `cast` AND imdb.rating > 9;"
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> plentifulDirectors() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> confusingMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector1(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> commentsOfDirector2(String director) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns true if the update was successful.
    public Boolean removeEarlyProjection(String movieId) {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    public List<JsonObject> nightMovies() {
        throw new UnsupportedOperationException("Not implemented, yet");
    }


}
