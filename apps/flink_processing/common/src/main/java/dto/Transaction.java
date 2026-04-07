package dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.flink.shaded.netty4.io.netty.util.internal.StringUtil;

import java.io.Serializable;
import java.math.BigDecimal;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Transaction implements Serializable {

    @JsonProperty("correlation_id")
    private String correlationId;

    private TransactionHeader transaction;

    private List<TransactionLine> lines;

    private Payment payment;

    private Status status;

    private Metadata metadata;

    public String getCorrelationId() { return correlationId; }
    public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }

    public TransactionHeader getTransaction() { return transaction; }
    public void setTransaction(TransactionHeader transaction) { this.transaction = transaction; }

    public List<TransactionLine> getLines() { return lines; }
    public void setLines(List<TransactionLine> lines) { this.lines = lines; }

    public Payment getPayment() { return payment; }
    public void setPayment(Payment payment) { this.payment = payment; }

    public Status getStatus() { return status; }
    public void setStatus(Status status) { this.status = status; }

    public Metadata getMetadata() { return metadata; }
    public void setMetadata(Metadata metadata) { this.metadata = metadata; }

    public String getTransactionId() {
        return transaction != null ? transaction.getTransactionId() : null;
    }

    public String toString() {
        return "Transaction{transactionId=" + getTransactionId()
                + ", correlationId=" + correlationId
                + ", lines=" + (lines != null ? lines.size() : 0)
                + "}";
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransactionHeader implements Serializable {
        @JsonProperty("transaction_id")
        private String transactionId;
        @JsonProperty("date")
        private String date;
        @JsonProperty("location_code")
        private String locationCode;
        @JsonProperty("identifier_no")
        private String identifierNo;
        @JsonProperty("pos_id")
        private String posId;
        @JsonProperty("printer_id")
        private String printerId;
        @JsonProperty("metadata_id")
        private String metadataId;
        @JsonProperty("currency_code")
        private String currencyCode;
        @JsonProperty("cashier_id")
        private String cashierId;
        @JsonProperty("creation_date")
        private String creationDate;
        @JsonProperty("correlation_id")
        private String correlationId;

        public String getTransactionId() { return transactionId; }
        public void setTransactionId(String transactionId) { this.transactionId = transactionId; }
        public String getDate() { return date; }
        public void setDate(String date) { this.date = date; }
        public String getLocationCode() { return locationCode; }
        public void setLocationCode(String locationCode) { this.locationCode = locationCode; }
        public String getIdentifierNo() { return identifierNo; }
        public void setIdentifierNo(String identifierNo) { this.identifierNo = identifierNo; }
        public String getPosId() { return posId; }
        public void setPosId(String posId) { this.posId = posId; }
        public String getPrinterId() { return printerId; }
        public void setPrinterId(String printerId) { this.printerId = printerId; }
        public String getMetadataId() { return metadataId; }
        public void setMetadataId(String metadataId) { this.metadataId = metadataId; }
        public String getCurrencyCode() { return currencyCode; }
        public void setCurrencyCode(String currencyCode) { this.currencyCode = currencyCode; }
        public String getCashierId() { return cashierId; }
        public void setCashierId(String cashierId) { this.cashierId = cashierId; }
        public String getCreationDate() { return creationDate; }
        public void setCreationDate(String creationDate) { this.creationDate = creationDate; }
        public String getCorrelationId() { return correlationId; }
        public void setCorrelationId(String correlationId) { this.correlationId = correlationId; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class TransactionLine implements Serializable {
        @JsonProperty("transaction_line_id")
        private String transactionLineId;
        @JsonProperty("prd_code")
        private String prdCode;
        @JsonProperty("quantity")
        private Integer quantity;
        @JsonProperty("unit_price_net")
        private BigDecimal unitPriceNet;
        @JsonProperty("line_net_value")
        private BigDecimal lineNetValue;
        @JsonProperty("total_line_value")
        private BigDecimal totalLineValue;
        @JsonProperty("total_tax_amount")
        private BigDecimal totalTaxAmount;
        @JsonProperty("discount_value")
        private BigDecimal discountValue;
        @JsonProperty("correlation_id")
        private String correlationId;
        @JsonProperty("tax_rate")
        private BigDecimal taxRate;


        public String getTransactionLineId() { return transactionLineId; }
        public void setTransactionLineId(String transactionLineId) { this.transactionLineId = transactionLineId; }
        public String getPrdCode() { return prdCode; }
        public void setPrdCode(String prdCode) { this.prdCode = prdCode; }
        public Integer getQuantity() { return quantity; }
        public void setQuantity(Integer quantity) { this.quantity = quantity; }
        public BigDecimal getUnitPriceNet() { return unitPriceNet; }
        public void setUnitPriceNet(BigDecimal unitPriceNet) { this.unitPriceNet = unitPriceNet; }
        public BigDecimal getLineNetValue() { return lineNetValue; }
        public void setLineNetValue(BigDecimal lineNetValue) { this.lineNetValue = lineNetValue; }
        public BigDecimal getTotalLineValue() { return totalLineValue; }
        public void setTotalLineValue(BigDecimal totalLineValue) { this.totalLineValue = totalLineValue; }
        public BigDecimal getTotalTaxAmount() { return totalTaxAmount; }
        public void setTotalTaxAmount(BigDecimal totalTaxAmount) { this.totalTaxAmount = totalTaxAmount; }
        public BigDecimal getDiscountValue() { return discountValue; }
        public void setDiscountValue(BigDecimal discountValue) { this.discountValue = discountValue; }

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }

        public void setTaxRate(BigDecimal taxRate) {
            this.taxRate = taxRate;
        }
        public BigDecimal getTaxRate() {
            return taxRate;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Payment implements Serializable {
        @JsonProperty("payment_id")
        private String paymentId;
        @JsonProperty("method")
        private String method;
        @JsonProperty("total_value")
        private BigDecimal totalValue;
        @JsonProperty("total_net_value")
        private BigDecimal totalNetValue;
        @JsonProperty("total_payment")
        private BigDecimal totalPayment;
        @JsonProperty("discount_value")
        private BigDecimal discountValue;
        @JsonProperty("correlation_id")
        private String correlationId;

        public String getPaymentId() { return paymentId; }
        public void setPaymentId(String paymentId) { this.paymentId = paymentId; }
        public String getMethod() { return method; }
        public void setMethod(String method) { this.method = method; }
        public BigDecimal getTotalValue() { return totalValue; }
        public void setTotalValue(BigDecimal totalValue) { this.totalValue = totalValue; }
        public BigDecimal getTotalNetValue() { return totalNetValue; }
        public void setTotalNetValue(BigDecimal totalNetValue) { this.totalNetValue = totalNetValue; }
        public BigDecimal getTotalPayment() { return totalPayment; }
        public void setTotalPayment(BigDecimal totalPayment) { this.totalPayment = totalPayment; }
        public BigDecimal getDiscountValue() { return discountValue; }
        public void setDiscountValue(BigDecimal discountValue) { this.discountValue = discountValue; }

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Status implements Serializable {
        @JsonProperty("transaction_status_id")
        private String transactionStatusId;
        @JsonProperty("status")
        private String status;
        @JsonProperty("is_current")
        private Boolean isCurrent;
        @JsonProperty("payment_status")
        private String paymentStatus;
        @JsonProperty("transaction_status")
        private String transactionStatus;
        @JsonProperty("cancelled")
        private Boolean cancelled;
        @JsonProperty("correlation_id")
        private String correlationId;

        public String getTransactionStatusId() { return transactionStatusId; }
        public void setTransactionStatusId(String transactionStatusId) { this.transactionStatusId = transactionStatusId; }
        public String getStatus() { return status; }
        public void setStatus(String status) { this.status = status; }
        public Boolean getCancelled() { return cancelled; }
        public void setCancelled(Boolean cancelled) { this.cancelled = cancelled; }
        public String getPaymentStatus() { return paymentStatus; }
        public void setPaymentStatus(String paymentStatus) { this.paymentStatus = paymentStatus; }
        public String getTransactionStatus() { return transactionStatus; }
        public void setTransactionStatus(String transactionStatus) { this.transactionStatus = transactionStatus; }
        public Boolean getIsCurrent() { return isCurrent; }
        public void setIsCurrent(Boolean isCurrent) { this.isCurrent = isCurrent; }

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Metadata implements Serializable {
        @JsonProperty("metadata_id")
        private String metadataId;
        private String comments;
        @JsonProperty("import_field")
        private String importField;
        @JsonProperty("shared")
        private Boolean shared;
        @JsonProperty("user_loan")
        private Boolean userLoan;
        @JsonProperty("correlation_id")
        private String correlationId;
        public String getMetadataId() { return metadataId; }
        public void setMetadataId(String metadataId) { this.metadataId = metadataId; }
        public String getComments() { return comments; }
        public void setComments(String comments) { this.comments = comments; }
        public String getImportField() { return importField; }
        public void setImportField(String importField) { this.importField = importField; }
        public Boolean getShared() { return shared; }
        public void setShared(Boolean shared) { this.shared = shared; }
        public Boolean getUserLoan() { return userLoan; }
        public void setUserLoan(Boolean userLoan) { this.userLoan = userLoan; }

        public String getCorrelationId() {
            return correlationId;
        }

        public void setCorrelationId(String correlationId) {
            this.correlationId = correlationId;
        }
    }
}
