package com.example.kafka.mapper;

import com.example.kafka.dto.MessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class CustomSerializer implements Serializer<MessageDto> {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, MessageDto data) {
        try {
            if (data == null){
                System.out.println("Non ho ricevuto nulla da serializzare");
                return null;
            }
            System.out.println("Serializzando...");
            return objectMapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new SerializationException("Errore durante la serializzazione di MessageDto in byte[]");
        }
    }

    @Override
    public void close() {
    }
}
