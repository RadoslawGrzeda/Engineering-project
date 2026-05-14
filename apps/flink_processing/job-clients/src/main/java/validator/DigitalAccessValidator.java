package validator; import config.SinkValidator;
import dto.Client;



import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class DigitalAccessValidator extends SinkValidator<Client> {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");


    @Override
    protected List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();
        Client.DigitalAccess da = client.getDigitalAccess();

        if (!isBlank(da.getEmailUser()) && !EMAIL_PATTERN.matcher(da.getEmailUser()).matches()) {
            errors.add("email_user invalid format: " + da.getEmailUser());
        }

        if (!isBlank(da.getLastLoginAt()) && parseDate(da.getLastLoginAt()) == null) {
            errors.add("last_login_date invalid format: " + da.getLastLoginAt());
        }

        if (!isBlank(da.getPortalUserConfirmationAt()) && parseDate(da.getPortalUserConfirmationAt()) == null) {
            errors.add("portal_user_confirmation_date invalid format: " + da.getPortalUserConfirmationAt());
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
        return "DIGITAL_ACCESS";
    }
}
