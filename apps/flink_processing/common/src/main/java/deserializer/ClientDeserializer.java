package deserializer;

import dto.Client;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.IOException;

public class ClientDeserializer implements DeserializationSchema<Client> {

    private static final Logger LOG = LoggerFactory.getLogger(ClientDeserializer.class);
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
        try {
            Client client = objectMapper.readValue(message, Client.class);
            if (client != null && client.getAccount() != null) {
                MDC.put("correlation_id", client.getAccount().getCorrelation_id());
            }
            MDC.put("service", "flink-clients");
            LOG.info("Deserialized client message");
            MDC.clear();
            return client;
        } catch (Exception e) {
            MDC.put("service", "flink-clients");
            LOG.error("Failed to deserialize client message: {}", new String(message), e);
            MDC.clear();
            return null;
        }
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
