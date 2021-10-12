package ch.heig.mac;

import java.util.List;

import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.json.JsonArray;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryResult;
import com.couchbase.client.java.query.QueryStatus;

import static com.couchbase.client.java.query.QueryOptions.queryOptions;


public class Requests
{
    private final Cluster cluster;

    public Requests(Cluster cluster)
    {
        this.cluster = cluster;
    }

    public List<String> getCollectionNames()
    {
        QueryResult result = cluster.query(
                "SELECT RAW r.name\n" +
                        "FROM system:keyspaces r\n" +
                        "WHERE r.`bucket` = \"mflix-sample\";"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> inconsistentRating()
    {
        QueryResult result = cluster.query(
                "SELECT imdb.id imdb_id, tomatoes.viewer.rating " +
                        "tomato_rating, imdb.rating imdb_rating\n" +
                        "FROM `mflix-sample`._default.movies\n" +
                        "WHERE tomatoes.viewer.rating != 0 AND ABS(tomatoes" +
                        ".viewer.rating - imdb.rating) > 7"
        );

        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> topReviewers()
    {
        QueryResult result = cluster.query(
                "SELECT name, COUNT(name) as cnt\n" +
                        "FROM `mflix-sample`._default.comments\n" +
                        "GROUP BY name\n" +
                        "ORDER BY cnt DESC\n" +
                        "LIMIT 10"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<String> greatReviewers()
    {
        QueryResult result = cluster.query(
                "SELECT RAW name \n" +
                        "FROM `mflix-sample`.`_default`.`comments` \n" +
                        "GROUP BY name \n" +
                        "HAVING COUNT(_id) > 300;"
        );
        return result.rowsAs(String.class);
    }

    public List<JsonObject> bestMoviesOfActor(String actor)
    {
        QueryResult result = cluster.query(
                "SELECT imdb.id, imdb.rating, `cast` \n" +

                         "FROM `mflix-sample`.`_default`.`movies` \n" +
                         "WHERE ? IN `cast` AND imdb.rating > 9;", queryOptions().parameters(JsonArray.from(actor))
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> plentifulDirectors()
    {
        QueryResult result = cluster.query(
                "SELECT director_name, COUNT(1) count_film\n" +
                        "FROM `mflix-sample`._default.movies\n" +
                        "UNNEST directors director_name\n" +
                        "GROUP BY director_name"
        );

        return result.rowsAs(JsonObject.class);
    }


    public List<JsonObject> confusingMovies() {
        QueryResult result = cluster.query(
                "SELECT _id AS movie_id, title \n" +
                         "FROM `mflix-sample`.`_default`.`movies` \n" +
                         "WHERE ARRAY_LENGTH(directors) > 20; \n"
        );
        return result.rowsAsObject();
    }

    public List<JsonObject> commentsOfDirector1(String director)
    {
        QueryResult result = cluster.query(
                "SELECT movies._id movie_id, comments.text\n" +
                        "FROM `mflix-sample`._default.movies\n" +
                        "JOIN `mflix-sample`._default.comments ON movies._id " +
                        "= comments.movie_id\n" +
                        "WHERE \"" + director + "\" IN movies.directors"
        );
        return result.rowsAs(JsonObject.class);
    }

    public List<JsonObject> commentsOfDirector2(String director)
    {
        throw new UnsupportedOperationException("Not implemented, yet");
    }

    // Returns true if the update was successful.
    public Boolean removeEarlyProjection(String movieId)
    {
        QueryResult result = cluster.query(
                "UPDATE `mflix-sample`.`_default`.theaters AS t \n" +
                         "SET t.schedule = ARRAY v FOR v IN t.schedule WHEN v.hourBegin > \"18:00:00\" END \n" +
                         "WHERE ANY v IN t.schedule SATISFIES v.hourBegin <= \"18:00:00\" AND v.movieId = ? END",
                queryOptions().parameters(JsonArray.from(movieId))
        );

        return result.metaData().status() == QueryStatus.SUCCESS;
    }

    public List<JsonObject> nightMovies()
    {
        QueryResult result = cluster.query(
                "SELECT ms.movieId movie_id, m.title \n" +
                        "FROM ( SELECT sched.movieId, ARRAY_AGG(sched.hourBegin) schedules \n" +
                               "FROM `mflix-sample`.`_default`.theaters t UNNEST t.schedule AS sched \n" +
                               "GROUP BY sched.movieId) AS ms \n" +
                        "JOIN `mflix-sample`.`_default`.movies m \n" +
                          "ON m._id = ms.movieId \n" +
                        "WHERE EVERY startTime IN ms.schedules SATISFIES startTime > \"18:00:00\" END" );

        return result.rowsAsObject();
    }


}
