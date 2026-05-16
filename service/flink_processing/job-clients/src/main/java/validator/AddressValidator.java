package validator; import config.SinkValidator;
import dto.Client;



import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

public class AddressValidator extends SinkValidator<Client.AddressChannel> {

    private static final List<String> VALID_COUNTRY_CODES =
            List.of("PL", "DE", "CZ", "SK", "UA", "LT");

    private static final Map<String, Pattern> ZIP_CODE_PATTERNS = Map.of(
            "PL", Pattern.compile("^\\d{2}-\\d{3}$"),
            "DE", Pattern.compile("^\\d{5}$"),
            "CZ", Pattern.compile("^\\d{3}\\s?\\d{2}$"),
            "SK", Pattern.compile("^\\d{3}\\s?\\d{2}$"),
            "UA", Pattern.compile("^\\d{5}$"),
            "LT", Pattern.compile("^LT-\\d{5}$")
    );

    @Override
    protected List<String> validate(Client.AddressChannel addr) {
        List<String> errors = new ArrayList<>();

        if (!isBlank(addr.getCountryCode())) {
            String code = addr.getCountryCode().toUpperCase();
            if (!VALID_COUNTRY_CODES.contains(code)) {
                errors.add("country_code invalid: " + code);
            } else if (!isBlank(addr.getAddressZipCode())) {
                Pattern zipPattern = ZIP_CODE_PATTERNS.get(code);
                if (zipPattern != null && !zipPattern.matcher(addr.getAddressZipCode()).matches()) {
                    errors.add("zip_code '" + addr.getAddressZipCode() + "' invalid for country " + code);
                }
            }
        }

        if (!isBlank(addr.getAddressStreet()) && isBlank(addr.getAddressCity())) {
            errors.add("street provided but city is missing");
        }

        return errors;
    }

    @Override
    protected String extractPersonId(Client.AddressChannel element) {
        return element.getPersonId();
    }

    @Override
    protected String extractCorrelationId(Client.AddressChannel element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String sinkName() {
        return "ADDRESS";
    }
}
