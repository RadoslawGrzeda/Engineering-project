package deserializer;

import dto.Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class ClientDeserializer implements DeserializationSchema<Client> {

    private transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) {
        this.objectMapper = new ObjectMapper();
    }

    @Override
    public Client deserialize(byte[] message) throws IOException {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        return objectMapper.readValue(message, Client.class);
    }

    @Override
    public boolean isEndOfStream(Client nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Client> getProducedType() {
        return TypeInformation.of(Client.class);
    }
}
