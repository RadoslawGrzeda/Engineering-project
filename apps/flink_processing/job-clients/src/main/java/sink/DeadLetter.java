package sink; import dto.Client;



import java.io.Serializable;

public class DeadLetter implements Serializable {
    private String personId;
    private String correlationId;
    private String sourceApplication;
    private String errorCode;
    private String errorMessage;
    private String rawPayload;

    public DeadLetter() {}

    public String getPersonId() { return personId; }
    public void setPersonId(String personId) { this.personId = personId; }

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public String getSourceApplication() { return sourceApplication; }
    public void setSourceApplication(String sourceApplication) { this.sourceApplication = sourceApplication; }

    public String getErrorCode() { return errorCode; }
    public void setErrorCode(String errorCode) { this.errorCode = errorCode; }

    public String getErrorMessage() { return errorMessage; }
    public void setErrorMessage(String errorMessage) { this.errorMessage = errorMessage; }

    public String getRawPayload() { return rawPayload; }
    public void setRawPayload(String rawPayload) { this.rawPayload = rawPayload; }
}
