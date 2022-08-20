package com.github.rephrasing.candle;

import org.bson.Document;

/**
 * Represents a CandleAdapter, A simple but powerful encoding/decoding interface used in converting a datatype to a {@link Document} and vice versa
 * @param <T> The Data type
 */
public interface CandleAdapter<T> {

    Document toDocument(T t);
    T fromDocument(Document document);

    Class<T> getType();

}
