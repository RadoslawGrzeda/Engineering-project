package validator; import config.SinkValidator;
import dto.Client;



import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ContactValidator extends SinkValidator<Client.ContactChannel> {

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");

    private static final Pattern PHONE_PATTERN =
            Pattern.compile("^[+]?[0-9\\s()-]{7,20}$");

    private static final List<String> VALID_CONTACT_TYPES =
            List.of("email", "phone", "sms");


    @Override
    protected List<String> validate(Client.ContactChannel contact) {
        List<String> errors = new ArrayList<>();

        if (isBlank(contact.getContactType())) {
            errors.add("contact_type is missing");
            return errors;
        }

        if (!VALID_CONTACT_TYPES.contains(contact.getContactType())) {
            errors.add("contact_type invalid: " + contact.getContactType());
        }

        if ("email".equals(contact.getContactType())) {
            if (isBlank(contact.getValue())) {
                errors.add("email value is missing");
            } else if (!EMAIL_PATTERN.matcher(contact.getValue()).matches()) {
                errors.add("email invalid format: " + contact.getValue());
            }
        } else if ("phone".equals(contact.getContactType()) || "sms".equals(contact.getContactType())) {
            if (!isBlank(contact.getValue()) && !PHONE_PATTERN.matcher(contact.getValue()).matches()) {
                errors.add("phone/sms invalid format: " + contact.getValue());
            }
        }

        return errors;
    }

    @Override
    protected String extractPersonId(Client.ContactChannel element) {
        return element.getPersonId();
    }

    @Override
    protected String extractCorrelationId(Client.ContactChannel element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String sinkName() {
        return "CONTACT";
    }
}
