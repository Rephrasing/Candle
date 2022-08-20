package com.github.rephrasing.candle.caching;

import com.github.rephrasing.candle.Candle;
import com.github.rephrasing.candle.CandleAdapter;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import lombok.Getter;
import org.bson.Document;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * This class MUST be a singleton class.
 * @param <T> the data type
 */
public abstract class CandleCacheHolder<T> {

    @Getter
    private final List<T> cached = new ArrayList<>();

    /**
     * Caches an object of the data type, only if it is not already cached.
     * @param t the object
     */
    public void cache(T t) {
        if (cached.contains(t)) throw new IllegalArgumentException("Attempted to cache a value but the value was already found in cache, if you are willing to replace it, please use CandleCacheHolder#cacheOrReplace(T)");
        cached.add(t);
    }

    /**
     * Caches an object of the data type, if it was found, it replaces it
     * @param t the object
     */
    public void cacheOrReplace(T t) {
        cached.remove(t);
        cached.add(t);
    }

    /**
     * Removes an object of the datatype if it was found.
     * @param t the object
     */
    public void remove(T t) {
        if (!cached.contains(t)) throw new IllegalArgumentException("Attempted to remove a value from cache but did not find the value");
        cached.remove(t);
    }

    /**
     * Retrieves a list of filtered (T)
     * @param filter the filter
     * @return a list of T
     */
    public List<T> getByFilter(Predicate<T> filter) {
        return cached.stream().filter(filter).collect(Collectors.toList());
    }


    /**
     * Retrieves the data as documents and parses it using a {@link CandleAdapter} for the provided data type
     * @param databaseName the database name
     * @param collectionName the collection name
     * @see CandleAdapter
     */
    public void retrieveFromDatabase(String databaseName, String collectionName) {
        MongoDatabase database = Candle.retrieve().getClient().getDatabase(databaseName);
        MongoCollection<Document> collection = database.getCollection(collectionName);

        CandleAdapter<T> adapter = Candle.retrieve().getAdapterByType(getType());
        if (adapter == null) throw new IllegalArgumentException("Attempted to retrieve data from database for CandleCacheHolder \"" + getClass().getName() + "\" but did not find a CandleAdapter for \"" + getType().getName() + "\"");

        collection.find().forEach(document -> cached.add(adapter.fromDocument(document)));
    }

    /**
     * Converts the current cached objects into Documents using a {@link CandleAdapter} for the provided data type and pushes them to the collection.
     * @param databaseName the database name
     * @param collectionName the collection name
     */
    public void pushToDatabase(String databaseName, String collectionName) {
        MongoDatabase database = Candle.retrieve().getClient().getDatabase(databaseName);
        database.getCollection(collectionName).drop();

        CandleAdapter<T> adapter = Candle.retrieve().getAdapterByType(getType());
        if (adapter == null) throw new IllegalArgumentException("Attempted to push data to database for CandleCacheHolder \"" + getClass().getName() + "\" but did not find a CandleAdapter for \"" + getType().getName() + "\"");

        List<Document> documented = cached.stream().map(adapter::toDocument).collect(Collectors.toList());

        database.getCollection(collectionName).insertMany(documented);
    }


    /**
     * The provided data type class
     * @return the class
     */
    public abstract Class<T> getType();


}
