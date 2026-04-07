package validator;

import com.fasterxml.jackson.databind.ObjectMapper;
//import config.DeadLetter;
import config.DeadLetter;
import dto.Transaction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class TransactionValidatorRequiredFields extends ProcessFunction<Transaction, Transaction> {

    public static final OutputTag<DeadLetter> DEAD_LETTER_TAG =
            new OutputTag<DeadLetter>("dead-letter") {};

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(TransactionValidatorRequiredFields.class);

    private static final Pattern TRANSACTION_ID_PATTERN =
            Pattern.compile("^[a-f0-9]{16}$");

    private static final Pattern LOCATION_CODE_PATTERN =
            Pattern.compile("^[A-Z]{2}\\d{3}$");

    private static final Pattern POS_ID_PATTERN =
            Pattern.compile("^POS-\\d{3}-\\d{3}$");

    private static final Pattern PRINTER_ID_PATTERN =
            Pattern.compile("^PRT-\\d{3}-\\d{3}$");

    private static final Pattern CASHIER_ID_PATTERN =
            Pattern.compile("^CSH-\\d{3}-\\d{3}$");

    private static final Pattern EAN_PATTERN =
            Pattern.compile("^\\d{13}$");

    private static final List<String> VALID_CURRENCY_CODES =
            List.of("PLN", "EUR", "CZK", "USD");

    private static final List<String> VALID_PAYMENT_METHODS =
            List.of("CARD", "CASH", "BLIK", "TRANSFER");

    private static final List<String> VALID_STATUSES =
            List.of("PENDING", "COMPLETED", "CANCELLED", "FAILED");

    public static List<String> validate(Transaction tx) {
        List<String> errors = new ArrayList<>();

        if (isBlank(tx.getCorrelationId())) {
            errors.add("correlation_id is missing");
        }

        if (tx.getTransaction() == null) {
            errors.add("transaction is null");
            return errors;
        }

        validateHeader(tx.getTransaction(), errors);
        validateLines(tx.getLines(), errors);
        validatePayment(tx.getPayment(), errors);
        validateStatus(tx.getStatus(), errors);
        validateMetadata(tx.getMetadata(), errors);

        return errors;
    }

    public static boolean isValid(Transaction tx) {
        return validate(tx).isEmpty();
    }

    private static void validateHeader(Transaction.TransactionHeader header, List<String> errors) {
        if (isBlank(header.getTransactionId())) {
            errors.add("transaction_id is missing");
        } else if (!TRANSACTION_ID_PATTERN.matcher(header.getTransactionId()).matches()) {
            errors.add("transaction_id invalid format: " + header.getTransactionId());
        }

        if (isBlank(header.getDate())) {
            errors.add("date is missing");
        } else if (parseDateTime(header.getDate()) == null) {
            errors.add("date invalid format: " + header.getDate());
        }

        if (isBlank(header.getLocationCode())) {
            errors.add("location_code is missing");
        } else if (!LOCATION_CODE_PATTERN.matcher(header.getLocationCode()).matches()) {
            errors.add("location_code invalid format: " + header.getLocationCode());
        }

        if (isBlank(header.getPosId())) {
            errors.add("pos_id is missing");
        } else if (!POS_ID_PATTERN.matcher(header.getPosId()).matches()) {
            errors.add("pos_id invalid format: " + header.getPosId());
        }

        if (isBlank(header.getPrinterId())) {
            errors.add("printer_id is missing");
        } else if (!PRINTER_ID_PATTERN.matcher(header.getPrinterId()).matches()) {
            errors.add("printer_id invalid format: " + header.getPrinterId());
        }

        if (isBlank(header.getCurrencyCode())) {
            errors.add("currency_code is missing");
        } else if (!VALID_CURRENCY_CODES.contains(header.getCurrencyCode())) {
            errors.add("currency_code invalid: " + header.getCurrencyCode());
        }

        if (isBlank(header.getCashierId())) {
            errors.add("cashier_id is missing");
        } else if (!CASHIER_ID_PATTERN.matcher(header.getCashierId()).matches()) {
            errors.add("cashier_id invalid format: " + header.getCashierId());
        }

        if (isBlank(header.getMetadataId())) {
            errors.add("metadata_id is missing");
        }

        if (!isBlank(header.getCreationDate()) && parseDateTime(header.getCreationDate()) == null) {
            errors.add("creation_date invalid format: " + header.getCreationDate());
        }
    }

    private static void validateLines(List<Transaction.TransactionLine> lines, List<String> errors) {
        if (lines == null || lines.isEmpty()) {
            errors.add("lines is empty");
            return;
        }

        for (int i = 0; i < lines.size(); i++) {
            Transaction.TransactionLine line = lines.get(i);
            String prefix = "lines[" + i + "] ";

            if (isBlank(line.getTransactionLineId())) {
                errors.add(prefix + "transaction_line_id is missing");
            }

            if (isBlank(line.getPrdCode())) {
                errors.add(prefix + "prd_code is missing");
            } else if (!EAN_PATTERN.matcher(line.getPrdCode()).matches()) {
                errors.add(prefix + "prd_code invalid EAN format: " + line.getPrdCode());
            }

            if (line.getQuantity() == null) {
                errors.add(prefix + "quantity is missing");
            } else if (line.getQuantity() <= 0) {
                errors.add(prefix + "quantity must be positive: " + line.getQuantity());
            }

            if (line.getUnitPriceNet() == null) {
                errors.add(prefix + "unit_price_net is missing");
            } else if (line.getUnitPriceNet().compareTo(BigDecimal.ZERO) < 0) {
                errors.add(prefix + "unit_price_net is negative");
            }

            if (line.getTotalLineValue() == null) {
                errors.add(prefix + "total_line_value is missing");
            } else if (line.getTotalLineValue().compareTo(BigDecimal.ZERO) < 0) {
                errors.add(prefix + "total_line_value is negative");
            }

            if (line.getTotalTaxAmount() == null) {
                errors.add(prefix + "total_tax_amount is missing");
            } else if (line.getTotalTaxAmount().compareTo(BigDecimal.ZERO) < 0) {
                errors.add(prefix + "total_tax_amount is negative");
            }

            if (line.getDiscountValue() != null && line.getDiscountValue().compareTo(BigDecimal.ZERO) < 0) {
                errors.add(prefix + "discount_value is negative");
            }

            if (line.getUnitPriceNet() != null && line.getTotalLineValue() != null
                    && line.getTotalLineValue().compareTo(line.getUnitPriceNet()) < 0
                    && (line.getDiscountValue() == null || line.getDiscountValue().compareTo(BigDecimal.ZERO) == 0)) {
                errors.add(prefix + "total_line_value less than unit_price_net without discount");
            }
        }
    }

    private static void validatePayment(Transaction.Payment payment, List<String> errors) {
        if (payment == null) {
            errors.add("payment is null");
            return;
        }

        if (isBlank(payment.getPaymentId())) {
            errors.add("payment_id is missing");
        }

        if (isBlank(payment.getMethod())) {
            errors.add("payment method is missing");
        } else if (!VALID_PAYMENT_METHODS.contains(payment.getMethod())) {
            errors.add("payment method invalid: " + payment.getMethod());
        }

        if (payment.getTotalValue() == null) {
            errors.add("payment total_value is missing");
        } else if (payment.getTotalValue().compareTo(BigDecimal.ZERO) < 0) {
            errors.add("payment total_value is negative");
        }

        if (payment.getTotalNetValue() == null) {
            errors.add("payment total_net_value is missing");
        } else if (payment.getTotalNetValue().compareTo(BigDecimal.ZERO) < 0) {
            errors.add("payment total_net_value is negative");
        }

        if (payment.getTotalValue() != null && payment.getTotalNetValue() != null
                && payment.getTotalNetValue().compareTo(payment.getTotalValue()) > 0) {
            errors.add("payment total_net_value exceeds total_value");
        }

        if (payment.getTotalPayment() == null) {
            errors.add("payment total_payment is missing");
        }

        if (payment.getDiscountValue() != null && payment.getDiscountValue().compareTo(BigDecimal.ZERO) < 0) {
            errors.add("payment discount_value is negative");
        }
    }

    private static void validateStatus(Transaction.Status status, List<String> errors) {
        if (status == null) {
            errors.add("status is null");
            return;
        }

        if (isBlank(status.getTransactionStatusId())) {
            errors.add("transaction_status_id is missing");
        }

        if (isBlank(status.getStatus())) {
            errors.add("status is missing");
        } else if (!VALID_STATUSES.contains(status.getStatus())) {
            errors.add("status invalid: " + status.getStatus());
        }

        if (isBlank(status.getPaymentStatus())) {
            errors.add("payment_status is missing");
        } else if (!VALID_STATUSES.contains(status.getPaymentStatus())) {
            errors.add("payment_status invalid: " + status.getPaymentStatus());
        }

        if (isBlank(status.getTransactionStatus())) {
            errors.add("transaction_status is missing");
        } else if (!VALID_STATUSES.contains(status.getTransactionStatus())) {
            errors.add("transaction_status invalid: " + status.getTransactionStatus());
        }

        if (status.getCancelled() == null) {
            errors.add("cancelled is missing");
        }

        if (status.getIsCurrent() == null) {
            errors.add("is_current is missing");
        }

        if (status.getCancelled() != null && status.getCancelled()
                && !"CANCELLED".equals(status.getStatus())) {
            errors.add("cancelled is true but status is not CANCELLED");
        }
    }

    private static void validateMetadata(Transaction.Metadata metadata, List<String> errors) {
        if (metadata == null) {
            errors.add("metadata is null");
            return;
        }

        if (isBlank(metadata.getMetadataId())) {
            errors.add("metadata_id is missing");
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static LocalDateTime parseDateTime(String dateTime) {
        if (isBlank(dateTime)) return null;
        try {
            return LocalDateTime.parse(dateTime);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    @Override
    public void processElement(Transaction tx, Context ctx, Collector<Transaction> out) throws Exception {
        MDC.put("service", "flink-transactions");
        MDC.put("correlation_id", tx.getCorrelationId() != null ? tx.getCorrelationId() : "-");
        MDC.put("transaction_id", tx.getTransactionId() != null ? tx.getTransactionId() : "-");
        try {
            LOG.info("Validating transaction {}", tx.getTransactionId());
            List<String> errors = validate(tx);
            if (errors.isEmpty()) {
                LOG.info("Transaction {} passed required fields validation", tx.getTransactionId());
                out.collect(tx);
            } else {
                LOG.warn("Transaction {} failed validation with {} errors: {}",
                        tx.getTransactionId(), errors.size(), errors);

                DeadLetter dl = new DeadLetter();
                dl.setTransactionId(tx.getTransactionId());
                dl.setCorrelationId(tx.getCorrelationId());
                dl.setTransactionDateFromString(tx.getTransaction() != null ? tx.getTransaction().getDate() : null);
                dl.setLocationCode(tx.getTransaction() != null ? tx.getTransaction().getLocationCode() : null);
                dl.setErrorCode("VALIDATION_ERROR");
                dl.setErrorMessage(String.join("; ", errors));
                try {
                    dl.setRawPayload(MAPPER.writeValueAsString(tx));
                } catch (Exception ex) {
                    LOG.error("Failed to serialize transaction {} for dead letter", tx.getTransactionId(), ex);
                    dl.setRawPayload("serialization_error");
                }

                ctx.output(DEAD_LETTER_TAG, dl);
            }
        } finally {
            MDC.clear();
        }
    }
}
