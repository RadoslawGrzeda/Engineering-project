package validator; import dto.Client;

import com.fasterxml.jackson.databind.ObjectMapper;
import sink.DeadLetter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.List;

public abstract class SinkValidator<T> extends ProcessFunction<T, T> {

    public static final OutputTag<DeadLetter> DEAD_LETTER_TAG =
            new OutputTag<DeadLetter>("sink-dead-letter") {};

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(SinkValidator.class);

    protected abstract List<String> validate(T element);
    protected abstract String extractPersonId(T element);
    protected abstract String extractCorrelationId(T element);
    protected abstract String sinkName();

    @Override
    public void processElement(T element, Context ctx, Collector<T> out) {
        MDC.put("service", "flink-clients");
        MDC.put("correlation_id", extractCorrelationId(element) != null ? extractCorrelationId(element) : "-");
        try {
            List<String> errors = validate(element);
            if (errors.isEmpty()) {
                LOG.info("[{}] Validation passed for person {}", sinkName(), extractPersonId(element));
                out.collect(element);
            } else {
                LOG.warn("[{}] Validation failed for person {}: {}",
                        sinkName(), extractPersonId(element), errors);

                DeadLetter dl = new DeadLetter();
                dl.setPersonId(extractPersonId(element));
                dl.setCorrelationId(extractCorrelationId(element));
                dl.setSourceApplication(sinkName());
                dl.setErrorCode("OPTIONAL_VALIDATION_ERROR");
                dl.setErrorMessage(String.join("; ", errors));
                try {
                    dl.setRawPayload(MAPPER.writeValueAsString(element));
                } catch (Exception e) {
                    dl.setRawPayload("serialization_error");
                }
                ctx.output(DEAD_LETTER_TAG, dl);
            }
        } finally {
            MDC.clear();
        }
    }

    protected static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    protected static LocalDate parseDate(String date) {
        if (isBlank(date)) return null;
        try {
            String datePart = date.contains(" ") ? date.split(" ")[0] : date;
            return LocalDate.parse(datePart);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
}
