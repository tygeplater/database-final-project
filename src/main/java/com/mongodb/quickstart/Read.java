package com.mongodb.quickstart;

import com.mongodb.client.*;
import com.mongodb.client.model.*;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.json.JsonObject;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;



public class Read {
    private static MongoCollection<Document> fact;
    private static MongoCollection<Document> actors;
    private static MongoCollection<Document> recordings;
    private static MongoCollection<Document> ratings;
    private static MongoCollection<Document> categories;
    private static MongoCollection<Document> directors;

    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase finalDB = mongoClient.getDatabase("Lab_3_Star");
            fact = finalDB.getCollection("Fact_Collection");
            actors = finalDB.getCollection("Dim_Actors");
            recordings = finalDB.getCollection("Dim_Recordings");
            ratings = finalDB.getCollection("Dim_Ratings");
            categories = finalDB.getCollection("Dim_Categories");
            directors = finalDB.getCollection("Dim_Directors");

            //query3();
            //query4();
            //query5();
            //query6();
            //query7();
            //query8();
            //query9();
        }
    }

    /*
    List the number of videos for each video category.
     */
    private static void query3(){
        AggregateIterable<Document> result = categories.aggregate(
                Arrays.asList(
                    Aggregates.lookup(
                            "Fact_Collection",
                            "_id",
                            "category_id",
                            "facts"
                    ),
                    Aggregates.lookup(
                            "Dim_Recordings",
                            "facts.recording_id",
                            "_id",
                            "recordings"
                    ),
                    Aggregates.unwind("$recordings", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                    Aggregates.group(new Document("_id", "$_id").append("name", "$name"), // Group by both `_id` and `category_name`
                            Accumulators.sum("movieCount", 1))
        ));

        // Print results
        for (Document doc : result){
            //System.out.println(doc.toJson());
            int numMovies = doc.getInteger("movieCount");
            Document movieDoc = (Document) doc.get("_id");
            String movieCategory = movieDoc.getString("name");
            System.out.println(movieCategory + ": " + numMovies);
        }
    }

    /*
    List the number of videos for each video category where the inventory is non-zero
    inventory is in recording
    categorys are in category
     */
    private static void query4(){
        AggregateIterable<Document> result = categories.aggregate(
                Arrays.asList(
                        Aggregates.lookup(
                                "Fact_Collection",
                                "_id",
                                "category_id",
                                "facts"
                        ),
                        Aggregates.lookup(
                                "Dim_Recordings",
                                "facts.recording_id",
                                "_id",
                                "recordings"
                        ),
                        Aggregates.unwind("$recordings", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                        Aggregates.match(Filters.gt("recordings.stock_count", 0)),
                        Aggregates.group(new Document("_id", "$_id").append("name", "$name"), // Group by both `_id` and `category_name`
                                Accumulators.sum("movieCount", 1))
                ));

        // Print results
        for (Document doc : result) {
            //System.out.println(doc.toJson());
            int numMovies = doc.getInteger("movieCount");
            Document movieDoc = (Document) doc.get("_id");
            String movieCategory = movieDoc.getString("name");
            System.out.println(movieCategory + ": " + numMovies);
        }
    }

    /*
    For each actor, list the video categories that actor has appeared in.
     */
    private static void query5(){
        // Perform Aggregation
        AggregateIterable<Document> result = actors.aggregate(Arrays.asList(
                // Join dim_actors with fact_collection
                Aggregates.lookup(
                        "Fact_Collection",
                        "_id",
                        "actor_id",
                        "facts"
                ),
                // Unwind the facts array
                Aggregates.unwind("$facts", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Join fact_collection with dim_category
                Aggregates.lookup(
                        "Dim_Categories",
                        "facts.category_id",
                        "_id",
                        "categories"
                ),
                // Group by actor with categories
                Aggregates.group(
                        "$name",
                        Accumulators.addToSet("categories", "$categories.name")
                )
        ));

        // Print results
        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }

    /*
    Which actors have appeared in movies in different video categories?
     */
    private static void query6(){
        AggregateIterable<Document> result = actors.aggregate(Arrays.asList(
                // Join dim_actors with fact_collection
                Aggregates.lookup(
                        "Fact_Collection",
                        "_id",
                        "actor_id",
                        "facts"
                ),
                // Unwind the facts array
                Aggregates.unwind("$facts", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Join fact_collection with dim_category
                Aggregates.lookup(
                        "Dim_Categories",         // Foreign collection
                        "facts.category_id",
                        "_id",
                        "categories"
                ),
                // Group by actor with categories
                Aggregates.group(
                        "$name",
                        Accumulators.addToSet("categories", "$categories.name")
                ),
                // Filter actors with multiple distinct categories
                Aggregates.match(Filters.expr(new Document("$gt", Arrays.asList(new Document("$size", "$categories"), 1))))
        ));

        // Print results
        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }

    /*
    Which actors have not appeared in a comedy?
     */
    private static void query7(){
        AggregateIterable<Document> result = actors.aggregate(Arrays.asList(
                // Join dim_actors with fact_collection
                Aggregates.lookup(
                        "Fact_Collection",
                        "_id",
                        "actor_id",
                        "facts"
                ),
                // Unwind the facts array
                Aggregates.unwind("$facts", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Join fact_collection with dim_category
                Aggregates.lookup(
                        "Dim_Categories",
                        "facts.category_id",
                        "_id",
                        "categories"
                ),
                // Unwind categories array to flatten structure
                Aggregates.unwind("$categories", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Group by actor with distinct categories
                Aggregates.group(
                        "$name",
                        Accumulators.addToSet("categories", "$categories.name")
                ),
                // Filter out actors who have been in the "Comedy" category
                Aggregates.match(Filters.not(Filters.in("categories", Arrays.asList("Comedy"))))
        ));

        // Print results
        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }

    /*
    Which actors have appeared in both a comedy and an action adventure movie?
     */
    private static void query8(){
        AggregateIterable<Document> result = actors.aggregate(Arrays.asList(
                // Join dim_actors with fact_collection
                Aggregates.lookup(
                        "Fact_Collection",
                        "_id",
                        "actor_id",
                        "facts"
                ),
                // Unwind the facts array
                Aggregates.unwind("$facts", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Join fact_collection with dim_category
                Aggregates.lookup(
                        "Dim_Categories",
                        "facts.category_id",
                        "_id",
                        "categories"
                ),
                // Unwind categories array to flatten structure
                Aggregates.unwind("$categories", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Group by actor with distinct categories
                Aggregates.group(
                        "$name",
                        Accumulators.addToSet("categories", "$categories.name")
                ),
                // Filter actors who have both "Comedy" and "Action & Adventure" in their categories
                Aggregates.match(Filters.and(
                        Filters.in("categories", Arrays.asList("Comedy")),
                        Filters.in("categories", Arrays.asList("Action & Adventure"))
                ))
        ));

        // Print results
        for (Document doc : result) {
            System.out.println(doc.toJson());
        }
    }

    private static void query9(){
        // How many actors are in an Action/Adventure movie that sells for over $20?
        AggregateIterable<Document> result = fact.aggregate(Arrays.asList(
                // Join with Dim_category to get category names
                Aggregates.lookup(
                        "Dim_Categories",
                        "category_id",
                        "_id",
                        "categories"
                ),
                Aggregates.unwind("$categories", new UnwindOptions().preserveNullAndEmptyArrays(false)),

                // Match categories that are Action or Adventure
                Aggregates.match(Filters.in("categories.name", Arrays.asList("Action & Adventure"))),

                // Join with Dim_recording to get prices
                Aggregates.lookup(
                        "Dim_Recordings",
                        "recording_id",
                        "_id",
                        "recordings"
                ),
                Aggregates.unwind("$recordings", new UnwindOptions().preserveNullAndEmptyArrays(false)),

                // Match recordings with price > 20
                Aggregates.match(Filters.gt("recordings.price", 20)),

                // Group by actor_id to count distinct actors
                Aggregates.group("$actor_id"),

                // Count the number of distinct actors
                Aggregates.group(null, Accumulators.sum("totalActors", 1))
        ));
        for (Document doc : result) {
            System.out.println("Number of actors in an Action & Adventure Movie that sells for more than $20: " + doc.get("totalActors"));
        }
    }
}












