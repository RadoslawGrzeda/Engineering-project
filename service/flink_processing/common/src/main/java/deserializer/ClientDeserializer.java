package deserializer;

import dto.Client;
import com.fasterxml.jackson.databind.JsonNode;
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
            JsonNode root = objectMapper.readTree(message);
            JsonNode payloadNode = (root.has("payload") && root.get("payload").isObject())
                    ? root.get("payload")
                    : root;

            Client client = objectMapper.treeToValue(payloadNode, Client.class);

            if (client != null) {
                client.setEventId(asTextOrNull(root, "event_id"));
                client.setEventType(asTextOrNull(root, "event_type"));
                client.setEventTimestamp(asTextOrNull(root, "event_timestamp"));
                client.setUpdateAction(asTextOrNull(root, "update_action"));
                client.setSourceSystem(asTextOrNull(root, "source_system"));
                client.setSchemaVersion(asTextOrNull(root, "schema_version"));

                if (client.getAccount() != null) {
                    MDC.put("correlation_id", client.getAccount().getCorrelation_id());
                }
            }
            MDC.put("service", "flink-clients");
            LOG.info("Deserialized client message (event_type={}, update_action={})",
                    asTextOrNull(root, "event_type"),
                    asTextOrNull(root, "update_action"));
            MDC.clear();
            return client;
        } catch (Exception e) {
            MDC.put("service", "flink-clients");
            LOG.error("Failed to deserialize client message: {}", new String(message), e);
            MDC.clear();
            return null;
        }
    }

    private static String asTextOrNull(JsonNode node, String field) {
        JsonNode v = node.get(field);
        return (v == null || v.isNull()) ? null : v.asText();
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
