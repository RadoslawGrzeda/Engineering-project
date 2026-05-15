package deserializer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dto.Client;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AddressChannelSerializer implements SerializationSchema<Client.AddressChannel> {

    private static final Logger LOG = LoggerFactory.getLogger(AddressChannelSerializer.class);
    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public byte[] serialize(Client.AddressChannel element) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            return objectMapper.writeValueAsBytes(element);
        } catch (JsonProcessingException e) {
            LOG.error("Failed to serialize AddressChannel for person_id={}: {}",
                    element.getPersonId(), e.getMessage());
            return new byte[0];
        }
    }
}
