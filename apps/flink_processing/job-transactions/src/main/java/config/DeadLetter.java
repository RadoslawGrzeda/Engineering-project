package config;


import java.io.Serializable;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DeadLetter implements Serializable {
    private String transactionId;
    private String correlationId;
    private Timestamp transactionDate;
    private String locationCode;
    private String errorCode;
    private String errorMessage;
    private String rawPayload;

    public DeadLetter() {}

    public String getTransactionId() { return transactionId; }
    public void setTransactionId(String transactionId) { this.transactionId = transactionId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public Timestamp getTransactionDate() { return transactionDate; }
    public void setTransactionDate(Timestamp transactionDate) { this.transactionDate = transactionDate; }

    public void setTransactionDateFromString(String dateStr) {
        if (dateStr != null && !dateStr.isEmpty()) {
            this.transactionDate = Timestamp.valueOf(LocalDateTime.parse(dateStr));
        }
    }

    public String getLocationCode() { return locationCode; }
    public void setLocationCode(String locationCode) { this.locationCode = locationCode; }

    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
}
