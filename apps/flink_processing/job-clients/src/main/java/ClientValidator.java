import dto.Client;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ClientValidator implements FlatMapFunction<Client, Client> {

    private static final Logger LOG = LoggerFactory.getLogger(ClientValidator.class);

    private static final Pattern EMAIL_PATTERN =
            Pattern.compile("^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$");

    private static final Pattern PHONE_PATTERN =
            Pattern.compile("^[+]?[0-9\\s()-]{7,20}$");

    private static final Pattern PERSON_ID_PATTERN =
            Pattern.compile("^[a-f0-9]{8}$");

    private static final Pattern ZIP_CODE_PATTERN =
            Pattern.compile("^\\d{2}-\\d{3}$");

    private static final List<String> VALID_GENDERS = List.of("M", "F");

    private static final List<String> VALID_CREATION_APPS =
            List.of("STORE_POS", "WEBSITE", "MOBILE_APPLICATION");

    private static final List<String> VALID_LOYALTY_STATUSES =
            List.of("Bronze", "Silver", "Gold", "Platinum");

    private static final List<String> VALID_CHANNEL_TYPES =
            List.of("email", "phone", "address");

    private static final List<String> VALID_INDICATOR_TYPES =
            List.of("SENIOR", "KDR", "EMPLOYEE");

    private static final List<String> VALID_INDICATOR_VALUES =
            List.of("ACTIVE", "NON_ACTIVE");

    private static final List<String> VALID_LANGUAGE_LEVELS =
            List.of("A1", "A2", "B1", "B2", "C1", "C2");

    public static List<String> validate(Client client) {
        List<String> errors = new ArrayList<>();

        if (client.getAccount() == null) {
            errors.add("account is null");
            return errors;
        }

        validateAccount(client.getAccount(), errors);
        validateLoyalty(client.getLoyalty(), errors);
        validateContactChannels(client.getContactChannels(), errors);
        validateAddressChannels(client.getAddressChannels(), errors);
        validateCommunicationSubscriptions(client.getCommunicationSubscriptions(), errors);
        validateDigitalAccess(client.getDigitalAccess(), errors);
        validateAccountIndicators(client.getAccountIndicators(), errors);
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

        if (isBlank(acc.getBirthDate().toString())) {
            errors.add("birth_date is missing");
        } else {
            LocalDate birth = acc.getBirthDate().toLocalDate();
            if (birth == null) {
                errors.add("birth_date invalid format: " + acc.getBirthDate());
            } else if (birth.isAfter(LocalDate.now())) {
                errors.add("birth_date is in the future");
            } else if (birth.isBefore(LocalDate.now().minusYears(120))) {
                errors.add("birth_date too old (>120 years)");
            }
        }

        if (isBlank(acc.getGenderCode())) {
            errors.add("gender_code is missing");
        } else if (!VALID_GENDERS.contains(acc.getGenderCode())) {
            errors.add("gender_code invalid: " + acc.getGenderCode());
        }

        if (isBlank(acc.getRegistrationDate().toString())) {
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


    private static void validateLoyalty(Client.Loyalty loyalty, List<String> errors) {
        if (loyalty == null) {
            errors.add("loyalty is null");
            return;
        }
        if (isBlank(loyalty.getLoyaltyStatus())) {
            errors.add("loyalty_status is missing");
        } else if (!VALID_LOYALTY_STATUSES.contains(loyalty.getLoyaltyStatus())) {
            errors.add("loyalty_status invalid: " + loyalty.getLoyaltyStatus());
        }

        LocalDate start = parseDate(loyalty.getStartDate());
        LocalDate end = parseDate(loyalty.getEndDate());
        if (start == null) {
            errors.add("loyalty start_date invalid");
        }
        if (end == null) {
            errors.add("loyalty end_date invalid");
        }
        if (start != null && end != null && end.isBefore(start)) {
            errors.add("loyalty end_date before start_date");
        }
    }


    private static void validateContactChannels(List<Client.ContactChannel> channels, List<String> errors) {
        if (channels == null || channels.isEmpty()) {
            errors.add("contact_channels is empty");
            return;
        }

        boolean hasValidEmail = false;
        boolean hasValidPhone = false;

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
                if (isBlank(ch.getValue())) {
                    errors.add(prefix + "phone value is missing");
                } else if (!PHONE_PATTERN.matcher(ch.getValue()).matches()) {
                    errors.add(prefix + "phone invalid format: " + ch.getValue());
                } else {
                    hasValidPhone = true;
                }
            }
        }

        if (!hasValidEmail) {
            errors.add("no valid email in contact_channels");
        }
        if (!hasValidPhone) {
            errors.add("no valid phone in contact_channels");
        }
    }


    private static void validateAddressChannels(List<Client.AddressChannel> channels, List<String> errors) {
        if (channels == null || channels.isEmpty()) {
            errors.add("address_channels is empty");
            return;
        }

        for (int i = 0; i < channels.size(); i++) {
            Client.AddressChannel ch = channels.get(i);
            String prefix = "address_channels[" + i + "] ";

            if (isBlank(ch.getAddressCity())) {
                errors.add(prefix + "city is missing");
            }
            if (isBlank(ch.getAddressAddress())) {
                errors.add(prefix + "address is missing");
            }
            if (isBlank(ch.getAddressZipCode())) {
                errors.add(prefix + "zip_code is missing");
            } else if (!ZIP_CODE_PATTERN.matcher(ch.getAddressZipCode()).matches()) {
                errors.add(prefix + "zip_code invalid format: " + ch.getAddressZipCode());
            }
            if (isBlank(ch.getAddressCode())) {
                errors.add(prefix + "address_code is missing");
            }
        }
    }


    private static void validateCommunicationSubscriptions(
            List<Client.CommunicationSubscription> subs, List<String> errors) {
        if (subs == null) {
            return; // subscriptions are optional
        }

        for (int i = 0; i < subs.size(); i++) {
            Client.CommunicationSubscription sub = subs.get(i);
            String prefix = "communication_subscriptions[" + i + "] ";

            if (isBlank(sub.getCommunityCode())) {
                errors.add(prefix + "community_code is missing");
            }

            LocalDate subDate = parseDate(sub.getDateOfSubscription());
            if (subDate == null) {
                errors.add(prefix + "date_of_subscription invalid");
            }

            if (!isBlank(sub.getDateOfUnsubscription())) {
                LocalDate unsubDate = parseDate(sub.getDateOfUnsubscription());
                if (unsubDate == null) {
                    errors.add(prefix + "date_of_unsubscription invalid format");
                } else if (subDate != null && unsubDate.isBefore(subDate)) {
                    errors.add(prefix + "unsubscription before subscription");
                }
            }
        }
    }


    private static void validateDigitalAccess(Client.DigitalAccess da, List<String> errors) {
        if (da == null) {
            errors.add("digital_access is null");
            return;
        }

        if (!isBlank(da.getEmailUser()) && !EMAIL_PATTERN.matcher(da.getEmailUser()).matches()) {
            errors.add("digital_access email invalid format: " + da.getEmailUser());
        }

        if (da.getIsActive() != null && da.getIsActive()) {
            if (isBlank(da.getUsername())) {
                errors.add("digital_access active but username is missing");
            }
        }
    }


    private static void validateAccountIndicators(Client.AccountIndicator ind, List<String> errors) {
        if (ind == null) {
            return; // nullable
        }

        if (isBlank(ind.getTypeAccountIndicator())) {
            errors.add("account_indicator type is missing");
        } else if (!VALID_INDICATOR_TYPES.contains(ind.getTypeAccountIndicator())) {
            errors.add("account_indicator type invalid: " + ind.getTypeAccountIndicator());
        }

        if (isBlank(ind.getValueAccountIndicator())) {
            errors.add("account_indicator value is missing");
        } else if (!VALID_INDICATOR_VALUES.contains(ind.getValueAccountIndicator())) {
            errors.add("account_indicator value invalid: " + ind.getValueAccountIndicator());
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
            // Handle both "yyyy-MM-dd" and "yyyy-MM-dd HH:mm:ss" formats
            String datePart = date.contains(" ") ? date.split(" ")[0] : date;
            return LocalDate.parse(datePart);
        } catch (DateTimeParseException e) {
            return null;
        }
    }
    @Override
    public void flatMap(Client client, Collector<Client> collector) throws Exception {
        List<String> errors = validate(client);
        if (errors.isEmpty()) {
            collector.collect(client);
        } else {
            LOG.warn("Client {} failed validation: {}", client.getPersonId(), errors);
            }
    }
}
