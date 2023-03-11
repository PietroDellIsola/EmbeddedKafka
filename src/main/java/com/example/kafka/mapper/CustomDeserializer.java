package com.example.kafka.mapper;

import com.example.kafka.dto.MessageDto;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

@Slf4j
public class CustomDeserializer implements Deserializer<MessageDto> {
    private ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public MessageDto deserialize(String topic, byte[] data) {
        try {
            if (data == null){
                System.out.println("Non ho ricevuto nulla da deserializzare");
                return null;
            }
            System.out.println("Deserializzando...");
            return objectMapper.readValue(new String(data, "UTF-8"), MessageDto.class);
        } catch (Exception e) {
            throw new SerializationException("Errore durante la deserializzazione di byte[] in MessageDto");
        }
    }

    @Override
    public void close() {
    }
}