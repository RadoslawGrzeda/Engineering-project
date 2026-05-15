package dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class
ClientEvent {

    @JsonProperty("event_id")
    private String eventId;

    @JsonProperty("event_type")
    private String eventType;

    @JsonProperty("event_timestamp")
    private String eventTimestamp;

    @JsonProperty("schema_version")
    private String schemaVersion;

    @JsonProperty("source_system")
    private String sourceSystem;

    @JsonProperty("update_action")
    private String updateAction;

    @JsonProperty("payload")
    private Client payload;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }

    public String getUpdateAction() { return updateAction; }
    public void setUpdateAction(String updateAction) { this.updateAction = updateAction; }

    public Client getPayload() { return payload; }
    public void setPayload(Client payload) { this.payload = payload; }
}
