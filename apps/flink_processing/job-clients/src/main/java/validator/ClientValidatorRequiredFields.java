package validator; import dto.Client;

import com.fasterxml.jackson.databind.ObjectMapper;

import config.DeadLetter;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ClientValidatorRequiredFields extends ProcessFunction<Client, Client> {

    public static final OutputTag<DeadLetter> DEAD_LETTER_TAG =
            new OutputTag<DeadLetter>("dead-letter") {};

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private static final Logger LOG = LoggerFactory.getLogger(ClientValidatorRequiredFields.class);

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");

    private static final Pattern PHONE_PATTERN =
            Pattern.compile("^[+]?[0-9\\s()-]{7,20}$");

    private static final Pattern PERSON_ID_PATTERN =
            Pattern.compile("^[a-f0-9]{12}$");

    private static final List<String> VALID_CREATION_APPS =
            List.of("STORE_POS", "WEBSITE", "MOBILE_APPLICATION");

    private static final List<String> VALID_LANGUAGE_LEVELS =
            List.of("A1", "A2", "B1", "B2", "C1", "C2");

    private static final List<String> VALID_COUNTRY_CODES=
            List.of("PL","DE","CZ","SK","UA","LT");

    public static List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();

        if (client.getAccount() == null) {
            errors.add("account is null");
            return errors;
        }

        validateAccount(client.getAccount(), errors);
        validateContactChannels(client.getContactChannels(), errors);
        validateLanguages(client.getLanguages(), errors);

        return errors;
    }

    public static boolean isValid(Client client) {
        return validate(client).isEmpty();
    }


    private static void validateAccount(Client.Account acc, List<String> errors) {
        if (isBlank(acc.getPersonId())) {
            errors.add("person_id is missing");
        } else if (!PERSON_ID_PATTERN.matcher(acc.getPersonId()).matches()) {
            errors.add("person_id invalid format: " + acc.getPersonId());
        }
        if (isBlank(acc.getCorrelation_id())) {
            errors.add("correlation_id is missing");
        }

        if (isBlank(acc.getFirstName())) {
            errors.add("first_name is missing");
        } else if (acc.getFirstName().length() > 100) {
            errors.add("first_name too long");
        }
        if (isBlank(acc.getLastName())) {
            errors.add("last_name is missing");
        } else if (acc.getLastName().length() > 100) {
            errors.add("last_name too long");
        }

        if (acc.getBirthDate() == null) {
            errors.add("birth_date is missing");
        } else {
            LocalDate birth = acc.getBirthDate().toLocalDate();
            if (birth.isAfter(LocalDate.now())) {
                errors.add("birth_date is in the future");
            } else if (birth.isBefore(LocalDate.now().minusYears(100))) {
                errors.add("birth_date too old (>100 years)");
            }
        }
        if (acc.getRegistrationDate() == null) {
            errors.add("registration_date is missing");
        } else if (parseDate(acc.getRegistrationDate().toString()) == null) {
            errors.add("registration_date invalid format: " + acc.getRegistrationDate());
        }

        if (isBlank(acc.getCreationApplication())) {
            errors.add("creation_application is missing");
        } else if (!VALID_CREATION_APPS.contains(acc.getCreationApplication())) {
            errors.add("creation_application invalid: " + acc.getCreationApplication());
        }
    }


    private static void validateContactChannels(List<Client.ContactChannel> channels, List<String> errors) {

        boolean hasValidEmail = false;
        if(channels == null || channels.isEmpty()) {
            errors.add("no valid email in contact_channels");
            return;
        }

        for (int i = 0; i < channels.size(); i++) {
            Client.ContactChannel ch = channels.get(i);
            String prefix = "contact_channels[" + i + "] ";

            if (isBlank(ch.getChannelType())) {
                errors.add(prefix + "channel_type is missing");
                continue;
            }

            if ("email".equals(ch.getChannelType())) {
                if (isBlank(ch.getValue())) {
                    errors.add(prefix + "email value is missing");
                } else if (!EMAIL_PATTERN.matcher(ch.getValue()).matches()) {
                    errors.add(prefix + "email invalid format: " + ch.getValue());
                } else {
                    hasValidEmail = true;
                }
            } else if ("phone".equals(ch.getChannelType())) {
                if (!isBlank(ch.getValue()) && !PHONE_PATTERN.matcher(ch.getValue()).matches()) {
                    errors.add(prefix + "phone invalid format: " + ch.getValue());
                }
            }
        }

        if (!hasValidEmail) {
            errors.add("no valid email in contact_channels");
        }
    }

    private static void validateLanguages(List<Client.Language> languages, List<String> errors) {
        if (languages == null || languages.isEmpty()) {
            errors.add("languages is empty");
            return;
        }

        for (int i = 0; i < languages.size(); i++) {
            Client.Language lang = languages.get(i);
            String prefix = "languages[" + i + "] ";

            if (isBlank(lang.getLanguageCode())) {
                errors.add(prefix + "language_code is missing");
            }
            if (isBlank(lang.getLanguageLevel())) {
                errors.add(prefix + "language_level is missing");
            } else if (!VALID_LANGUAGE_LEVELS.contains(lang.getLanguageLevel())) {
                errors.add(prefix + "language_level invalid: " + lang.getLanguageLevel());
            }
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static LocalDate parseDate(String date) {
        if (isBlank(date)) return null;
        try {
            String datePart = date.contains(" ") ? date.split(" ")[0] : date;
            return LocalDate.parse(datePart);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
    @Override
    public void processElement(Client client, Context ctx, Collector<Client> out) throws Exception {
        MDC.put("service", "flink-clients");
        MDC.put("correlation_id", client.getAccount() != null ? client.getAccount().getCorrelation_id() : "-");
        try {
            List<String> errors = validate(client);
            if (errors.isEmpty()) {
                LOG.info("Client {} passed required fields validation", client.getPersonId());
                out.collect(client);
            } else {
                LOG.warn("Client {} failed validation: {}", client.getPersonId(), errors);

                DeadLetter dl = new DeadLetter();
                dl.setPersonId(client.getAccount() != null ? client.getAccount().getPersonId() : null);
                dl.setCorrelationId(client.getAccount() != null ? client.getAccount().getCorrelation_id() : null);
                dl.setSourceApplication(client.getAccount() != null ? client.getAccount().getCreationApplication() : null);
                dl.setErrorCode("VALIDATION_ERROR");
                dl.setErrorMessage(String.join("; ", errors));
                dl.setRawPayload(MAPPER.writeValueAsString(client));

                ctx.output(DEAD_LETTER_TAG, dl);
            }
        } finally {
            MDC.clear();
        }
    }
}
