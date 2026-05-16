package validator; import config.SinkValidator;
import dto.Client;



import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class CommunicationSubscriptionValidator extends SinkValidator<Client.CommunicationSubscription> {


    @Override
    protected List<String> validate(Client.CommunicationSubscription comm) {
        List<String> errors = new ArrayList<>();

        if (isBlank(comm.getCommunicationCode())) {
            errors.add("communication_code is missing");
        }

        if (!isBlank(comm.getDateOfSubscription()) && parseDate(comm.getDateOfSubscription()) == null) {
            errors.add("date_of_subscription invalid format: " + comm.getDateOfSubscription());
        }

        if (!isBlank(comm.getDateOfUnsubscription()) && parseDate(comm.getDateOfUnsubscription()) == null) {
            errors.add("date_of_unsubscription invalid format: " + comm.getDateOfUnsubscription());
        }

        if (!isBlank(comm.getDateOfSubscription()) && !isBlank(comm.getDateOfUnsubscription())) {
            LocalDate sub = parseDate(comm.getDateOfSubscription());
            LocalDate unsub = parseDate(comm.getDateOfUnsubscription());
            if (sub != null && unsub != null && unsub.isBefore(sub)) {
                errors.add("date_of_unsubscription is before date_of_subscription");
            }
        }

        return errors;
    }

    @Override
    protected String extractPersonId(Client.CommunicationSubscription element) {
        return element.getPersonId();
    }

    @Override
    protected String extractCorrelationId(Client.CommunicationSubscription element) {
        return element.getCorrelation_id();
    }

    @Override
    protected String sinkName() {
        return "COMMUNICATION_SUBSCRIPTION";
    }
}
