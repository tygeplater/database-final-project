package com.mongodb.quickstart;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.InsertManyOptions;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;

import java.util.*;

import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Filters.gte;

public class Create {
    public static void main(String[] args) {
        try (MongoClient mongoClient = MongoClients.create(System.getProperty("mongodb.uri"))) {
            MongoDatabase originalDB = mongoClient.getDatabase("Lab_3");
            MongoDatabase finalDB = mongoClient.getDatabase("Lab_3_Star");

            createVideoRecordingCollection(originalDB, finalDB);
            createCategoriesCollection(originalDB, finalDB);
            createActorsCollection(originalDB, finalDB);
            createRatingsCollection(originalDB, finalDB);
            createDirectorCollection(originalDB, finalDB);
            createInventoryCollection(originalDB, finalDB);
            createFactTable(originalDB, finalDB);
        }
    }

    private static void createInventoryCollection(MongoDatabase originalDB, MongoDatabase finalDB) {
        System.out.println("creating inventory collection");
        MongoCollection<Document> originalCollection = originalDB.getCollection("Video_Recordings");
        MongoCollection<Document> newCollection = finalDB.getCollection("Dim_Inventory");
        newCollection.deleteMany(new Document());

        List<Document> recordingData = new ArrayList<>();
        for (Document recordings : originalCollection.find()) {
            Document recordingEntry = new Document("recording_id", recordings.getInteger("recording_id"))
                    .append("stock_count", recordings.getInteger("stock_count"))
                    .append("price", recordings.getInteger("price"));
            recordingData.add(recordingEntry);
        }
        // Insert into the fact collection
        newCollection.insertMany(recordingData);
    }

    private static void createFactTable(MongoDatabase originalDB, MongoDatabase finalDB){
        System.out.println("...fact table not implemented yet...");
    }

    private static void createDirectorCollection(MongoDatabase originalDB, MongoDatabase finalDB) {
        System.out.println("creating director collection");
        MongoCollection<Document> originalCollection = originalDB.getCollection("Video_Recordings");
        MongoCollection<Document> newCollection = finalDB.getCollection("Dim_Directors");

        newCollection.deleteMany(new Document());

        List<String> directorNames = new ArrayList<>();
        for(Document movie: originalCollection.find()){
            if(!directorNames.contains(movie.getString("director"))){
                directorNames.add(movie.getString("director"));
            }
        }

        List<Document> directors = new ArrayList<>();
        for(String directorName : directorNames){
            //Filter by actor name
            Bson filter = eq("director", directorName);

            //Create a list of recording ids by searching the actors name and adding associated ids
            List<Integer> recording_ids = new ArrayList<>();
            originalCollection.find(filter).forEach(doc -> {
                recording_ids.add(doc.getInteger("recording_id"));
            });

            Document directorEntry = new Document("director", directorName)
                    .append("Movies", recording_ids);
            directors.add(directorEntry);

        }
        //Insert into the new actor collection
        newCollection.insertMany(directors);
    }

    private static void createRatingsCollection(MongoDatabase originalDB, MongoDatabase finalDB){
        System.out.println("creating ratings collection");
        MongoCollection<Document> originalCollection = originalDB.getCollection("Video_Recordings");
        MongoCollection<Document> newCollection = finalDB.getCollection("Dim_Ratings");
        newCollection.deleteMany(new Document());
        List<String> ratings = Arrays.asList("PG", "PG-13", "R", "NR");

        List<Document> ratingData = new ArrayList<>();
        for(String rating : ratings){
            Bson filter = eq("rating", rating);

            List<Integer> recording_ids = new ArrayList<>();
            originalCollection.find(filter).forEach(doc -> {
                recording_ids.add(doc.getInteger("recording_id"));
            });

            Document ratingEntry = new Document("rating", rating)
                    .append("recording_ids", recording_ids);
            ratingData.add(ratingEntry);
        }

        newCollection.insertMany(ratingData);
    }

    private static void createActorsCollection(MongoDatabase originalDB, MongoDatabase finalDB){
        System.out.println("creating actors collection");
        MongoCollection<Document> originalCollection = originalDB.getCollection("Video_Actors");
        MongoCollection<Document> newCollection = finalDB.getCollection("Dim_Actors");
        newCollection.deleteMany(new Document());


        Map<String, Integer> actorIdMap = new TreeMap<>();
        for(Document actorDoc: originalCollection.find()){
            if(!actorIdMap.containsKey(actorDoc.getString("name"))) {
                actorIdMap.put(actorDoc.getString("name"), actorDoc.getInteger("id"));
            }
        }

        List<Document> actorData = new ArrayList<>();
        for(String actorName : actorIdMap.keySet()){
            //Filter by actor name
            Bson filter = eq("name", actorName);

            //Create a list of recording ids by searching the actors name and adding associated ids
            List<Integer> recording_ids = new ArrayList<>();
            originalCollection.find(filter).forEach(doc -> {
                recording_ids.add(doc.getInteger("recording_id"));
            });

            Document actorEntry = new Document("id", actorIdMap.get(actorName))
                    .append("name", actorName)
                    .append("Movies", recording_ids);
            actorData.add(actorEntry);
        }
        //Insert into the new actor collection
        newCollection.insertMany(actorData);
    }

    private static void createCategoriesCollection(MongoDatabase originalDB, MongoDatabase finalDB){
        System.out.println("creating categories collection");
        MongoCollection<Document> originalCollection = originalDB.getCollection("Video_Categories");
        MongoCollection<Document> newCollection = finalDB.getCollection("Dim_Categories");
        newCollection.deleteMany(new Document());

        List<Document> categoryData = new ArrayList<>();
        for(Document categories : originalCollection.find()){
            Document categoryEntry = new Document("category_id", categories.getInteger("id"))
                    .append("name", categories.getString("name"));
            categoryData.add(categoryEntry);
        }
        // Insert into the category collection
        newCollection.insertMany(categoryData);
    }

    private static void createVideoRecordingCollection(MongoDatabase originalDB, MongoDatabase finalDB) {
        System.out.println("creating recordings collection");
        MongoCollection<Document> originalCollection = originalDB.getCollection("Video_Recordings");
        MongoCollection<Document> newCollection = finalDB.getCollection("Dim_Recordings");
        newCollection.deleteMany(new Document());

        List<Document> recordingData = new ArrayList<>();
        for (Document recordings : originalCollection.find()) {
            Document recordingEntry = new Document("recording_id", recordings.getInteger("recording_id"))
                    .append("title", recordings.getString("title"))
                    .append("image_name", recordings.getString("image_name"))
                    .append("duration", recordings.getInteger("duration"))
                    .append("year_released", recordings.getInteger("year_released"));
            recordingData.add(recordingEntry);
        }
        // Insert into the fact collection
        newCollection.insertMany(recordingData);
    }
}