package com.github.rephrasing.candle;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import org.bson.codecs.configuration.CodecRegistry;
import org.bson.codecs.pojo.PojoCodecProvider;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

import static org.bson.codecs.configuration.CodecRegistries.fromProviders;
import static org.bson.codecs.configuration.CodecRegistries.fromRegistries;


/**
 * The main class for Candle
 * This is where everything is controlled.
 * <p>
 * This is a singleton class, therefore cannot be instantiated. Use {@link Candle#retrieve()} to get the single instance.
 * </p>
 */
public class Candle {

    private static Candle instance;

    private MongoClient client;
    private final List<CandleAdapter<?>> adapters = new ArrayList<>();

    private Candle() {}


    public static Candle retrieve() {
        if (instance == null) instance = new Candle();
        return instance;
    }

    /**
     * Retrieves a {@link CandleAdapter} by its data type
     * @param type the class of the type
     * @param <T> the type
     * @return a {@link CandleAdapter} if it exists, otherwise null
     */
    public <T> CandleAdapter<T> getAdapterByType(Class<T> type) {
        return (CandleAdapter<T>) adapters.stream().filter(adapter -> adapter.getType().equals(type)).findFirst().orElse(null);
    }

    /**
     * Registers {@link CandleAdapter}(s)
     * @param adapters the adapter(s)
     */
    public void registerAdapters(CandleAdapter<?>... adapters) {
        for (CandleAdapter<?> adapter : adapters) {
            CandleAdapter<?> foundSameType = getAdapterByType(adapter.getType());
            if (foundSameType != null) throw new IllegalArgumentException("Attempted to register CandleAdapter \"" + adapter.getClass().getName() + "\" for class \"" + adapter.getType().getName() + "\" but found a CandleAdapter for the same type. found: \"" + foundSameType.getClass().getName() + "\"");
            this.adapters.add(adapter);
        }
    }

    /**
     * Connects to MongoDB using a token
     * @param token the token
     */
    public void connect(String token) {
        if (client != null) throw new IllegalArgumentException("Attempted to connect to MongoDB twice!");

        CodecRegistry pojoCodecRegistry = fromProviders(PojoCodecProvider.builder().automatic(true).build());
        CodecRegistry defaultRegistry = fromRegistries(MongoClientSettings.getDefaultCodecRegistry(), pojoCodecRegistry);

        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(new ConnectionString(token))
                .codecRegistry(pojoCodecRegistry)
                .codecRegistry(defaultRegistry)
                .build();

        client = MongoClients.create(settings);
    }

    /**
     * Disconnects from MongoDB
     */
    public void disconnect() {
        if (client == null) throw new IllegalArgumentException("Attempted to disconnect from MongoDB but did not find a connection");
        client.close();
    }


    /**
     * Retrieves the {@link MongoClient} if a connection was found
     * @return the MongoClient
     */
    @NotNull
    public MongoClient getClient() {
        if (client == null) throw new IllegalArgumentException("Attempted to use MongoClient but did not find a connection");
        return client;
    }
}
