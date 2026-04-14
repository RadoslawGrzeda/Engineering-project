package validator; import config.SinkValidator;
import dto.Client;



import java.util.ArrayList;
import java.util.List;

public class CustomerIndicatorValidator extends SinkValidator<Client> {

    @Override
    protected List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();

        Client.AccountIndicator indicator = client.getAccountIndicators();
        if (isBlank(indicator.getTypeAccountIndicator())) {
            errors.add("indicator type is missing");
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
        return "CUSTOMER_INDICATOR";
    }

    @Override
    protected Client getRawPayload(Client element) {
        return element;
    }
}
