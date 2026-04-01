package validator; import dto.Client;



import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class LoyaltyValidator extends SinkValidator<Client> {

    private static final List<String> VALID_LOYALTY_STATUSES =
            List.of("BRONZE", "SILVER", "GOLD", "PLATINUM");

    @Override
    protected List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();
        Client.Loyalty loyalty = client.getLoyalty();

        if (isBlank(loyalty.getLoyaltyStatus())) {
            errors.add("loyalty status_code is missing");
        } else if (!VALID_LOYALTY_STATUSES.contains(loyalty.getLoyaltyStatus())) {
            errors.add("loyalty status_code invalid: " + loyalty.getLoyaltyStatus());
        }

        if (!isBlank(loyalty.getStartDate()) && parseDate(loyalty.getStartDate()) == null) {
            errors.add("start_date invalid format: " + loyalty.getStartDate());
        }

        if (!isBlank(loyalty.getEndDate()) && parseDate(loyalty.getEndDate()) == null) {
            errors.add("end_date invalid format: " + loyalty.getEndDate());
        }

        if (!isBlank(loyalty.getStartDate()) && !isBlank(loyalty.getEndDate())) {
            LocalDate start = parseDate(loyalty.getStartDate());
            LocalDate end = parseDate(loyalty.getEndDate());
            if (start != null && end != null && end.isBefore(start)) {
                errors.add("end_date is before start_date");
            }
        }

        return errors;
    }

    @Override
    protected String extractPersonId(Client element) {
        return element.getPersonId();
    }

    @Override
    protected String extractCorrelationId(Client element) {
        return element.getAccount().getCorrelation_id();
    }

    @Override
    protected String sinkName() {
        return "LOYALTY_STATUS";
    }
}
