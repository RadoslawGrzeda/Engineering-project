package deserializer;

import com.fasterxml.jackson.databind.ObjectMapper;
import dto.Transaction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.util.logging.Logger;

public class TransactionDeserializer implements DeserializationSchema<Transaction> {
    private static final Logger LOG = Logger.getLogger(TransactionDeserializer.class.getName());
    private  transient ObjectMapper objectMapper;

    @Override
    public void open(InitializationContext context) throws Exception {
        if (objectMapper == null) {
            this.objectMapper = new ObjectMapper();
        }
    }

    @Override
    public Transaction deserialize(byte[] message) throws IOException {
        if (this.objectMapper == null) {
            this.objectMapper = new ObjectMapper();
        }
        try {
            Transaction transaction = objectMapper.readValue(message, Transaction.class);
            if (transaction != null && transaction.getCorrelationId() != null) {
                LOG.info("Deserialized transaction message");
                return transaction;
            }
        } catch (IOException e) {
            LOG.warning("Error deserializing transaction message: " + e.getMessage());
            throw e;
        }
        return null;
    }


    @Override
    public boolean isEndOfStream(Transaction nextElement) {
        return false;
    }

    @Override
    public TypeInformation<Transaction> getProducedType() {
        return TypeInformation.of(Transaction.class);
    }
}
