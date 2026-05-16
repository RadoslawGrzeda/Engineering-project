package validator; import config.SinkValidator;
import dto.Client;



import java.util.ArrayList;
import java.util.List;

public class NationalityValidator extends SinkValidator<Client.Nationality> {

    private static final List<String> VALID_COUNTRY_CODES =
            List.of("PL", "DE", "CZ", "SK", "UA", "LT");


    @Override
    protected List<String> validate(Client.Nationality nat) {
        List<String> errors = new ArrayList<>();

        if (isBlank(nat.getCountryCode())) {
            errors.add("country_code is missing");
        } else if (!VALID_COUNTRY_CODES.contains(nat.getCountryCode().toUpperCase())) {
            errors.add("country_code invalid: " + nat.getCountryCode());
        }

        return errors;
    }

    @Override
    protected String extractPersonId(Client.Nationality element) {
        return element.getPersonId();
    }

    @Override
    protected String extractCorrelationId(Client.Nationality element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String sinkName() {
        return "NATIONALITY";
    }
}
