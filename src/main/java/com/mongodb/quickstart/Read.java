package com.mongodb.quickstart;

import com.mongodb.client.*;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Field;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.UnwindOptions;
import org.bson.Document;

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

            query1();

            /* examples:
            // find one document with Filters.eq()
            Document student2 = gradesCollection.find(eq("student_id", 10000)).first();
            System.out.println("Student 2: " + student2.toJson());

            // find a list of documents and use a List object instead of an iterator
            List<Document> studentList = gradesCollection.find(gte("student_id", 10000)).into(new ArrayList<>());
            System.out.println("Student list with an ArrayList:");
            for (Document student : studentList) {
                System.out.println(student.toJson());
            }
             */
        }
    }

    private static void query1() {
        // Perform Aggregation
        AggregateIterable<Document> result = recordings.aggregate(Arrays.asList(
                // Step 1: Join with fact_collection
                Aggregates.lookup(
                        "Fact_Collection",  // Foreign collection
                        "_id",              // Local field in dim_recordings
                        "recording_id",     // Foreign field in fact_collection
                        "facts"             // Resulting field
                ),
                // Step 2: Unwind the facts array NOTE FROM DAVID: not really sure what this does
                Aggregates.unwind("$facts", new UnwindOptions().preserveNullAndEmptyArrays(false)),
                // Step 3: Join with dim_category
                Aggregates.lookup(
                        "Dim_Categories",     // Foreign collection
                        "facts.category_id", // Field in facts
                        "_id",              // Field in dim_category
                        "categories"        // Resulting field
                ),
                // Step 4: create a new field called category_name and set it to the category
                Aggregates.addFields(Collections.singletonList(
                        new Field<>("categories_name", "$categories.name")
                )),
                // Step 5: Remove the fact and category fields to simplify output
                Aggregates.project(Projections.exclude("facts", "categories")) // Remove facts if still present
        ));


        // Print results
        int length = 0;
        for (Document doc : result) {
            System.out.println(doc.toJson());
            length++;
        }
        System.out.println("number of results: " + length);

    }
}
