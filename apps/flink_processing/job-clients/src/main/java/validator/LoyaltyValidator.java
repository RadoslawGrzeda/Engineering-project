package validator; import config.SinkValidator;
import dto.Client;



import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class LoyaltyValidator extends SinkValidator<Client> {

    private static final List<String> VALID_LOYALTY_STATUSES =
            List.of("Bronze", "Silver", "Gold", "Platinum");

    @Override
    protected List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();
        Client.Loyalty loyalty = client.getLoyalty();

        if (isBlank(loyalty.getStatusCode())) {
            errors.add("loyalty status_code is missing");
        } else if (!VALID_LOYALTY_STATUSES.contains(loyalty.getStatusCode())) {
            errors.add("loyalty status_code invalid: " + loyalty.getStatusCode());
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
