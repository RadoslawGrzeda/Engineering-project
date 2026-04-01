package validator; import dto.Client;



import java.util.ArrayList;
import java.util.List;

public class CivilValidator extends SinkValidator<Client> {

    private static final List<String> VALID_CIVIL_STATUSES =
            List.of("SINGLE", "MARRIED", "DIVORCED", "WIDOWED", "SEPARATED");

    @Override
    protected List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();

        String civilStatus = client.getAccount().getCivilStatus();
        if (isBlank(civilStatus)) {
            errors.add("civil_status is missing");
        } else if (!VALID_CIVIL_STATUSES.contains(civilStatus)) {
            errors.add("civil_status invalid: " + civilStatus);
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
        return "CIVIL";
    }
}
