package dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Client implements Serializable {

    private String eventId;
    private String eventType;
    private String eventTimestamp;
    private String updateAction;
    private String sourceSystem;
    private String schemaVersion;

    private Account account;
    private Loyalty loyalty;

    private List<Nationality> nationalities;

    @JsonProperty("address_channels")
    private List<AddressChannel> addressChannels;

    @JsonProperty("contact_channels")
    private List<ContactChannel> contactChannels;

    @JsonProperty("communication_subscriptions")
    private List<CommunicationSubscription> communicationSubscriptions;

    @JsonProperty("digital_access")
    private DigitalAccess digitalAccess;

    @JsonProperty("account_indicators")
    private AccountIndicator accountIndicators;

    private List<Language> languages;

    public String getEventId() { return eventId; }
    public void setEventId(String eventId) { this.eventId = eventId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public String getEventTimestamp() { return eventTimestamp; }
    public void setEventTimestamp(String eventTimestamp) { this.eventTimestamp = eventTimestamp; }

    public String getUpdateAction() { return updateAction; }
    public void setUpdateAction(String updateAction) { this.updateAction = updateAction; }

    public String getSourceSystem() { return sourceSystem; }
    public void setSourceSystem(String sourceSystem) { this.sourceSystem = sourceSystem; }

    public String getSchemaVersion() { return schemaVersion; }
    public void setSchemaVersion(String schemaVersion) { this.schemaVersion = schemaVersion; }

    public Account getAccount() { return account; }
    public void setAccount(Account account) { this.account = account; }

    public Loyalty getLoyalty() { return loyalty; }
    public void setLoyalty(Loyalty loyalty) { this.loyalty = loyalty; }

    public List<Nationality> getNationalities() { return nationalities; }
    public void setNationalities(List<Nationality> nationalities) { this.nationalities = nationalities; }

    public List<AddressChannel> getAddressChannels() { return addressChannels; }
    public void setAddressChannels(List<AddressChannel> addressChannels) { this.addressChannels = addressChannels; }

    public List<ContactChannel> getContactChannels() { return contactChannels; }
    public void setContactChannels(List<ContactChannel> contactChannels) { this.contactChannels = contactChannels; }

    public List<CommunicationSubscription> getCommunicationSubscriptions() { return communicationSubscriptions; }
    public void setCommunicationSubscriptions(List<CommunicationSubscription> communicationSubscriptions) { this.communicationSubscriptions = communicationSubscriptions; }

    public DigitalAccess getDigitalAccess() { return digitalAccess; }
    public void setDigitalAccess(DigitalAccess digitalAccess) { this.digitalAccess = digitalAccess; }

    public AccountIndicator getAccountIndicators() { return accountIndicators; }
    public void setAccountIndicators(AccountIndicator accountIndicators) { this.accountIndicators = accountIndicators; }

    public List<Language> getLanguages() { return languages; }
    public void setLanguages(List<Language> languages) { this.languages = languages; }

    public String getPersonId() {
        return account != null ? account.getPersonId() : null;
    }

    public String toString() {
        return "Client{personId=" + getPersonId()
                + ", name=" + (account != null ? account.getFirstName() + " " + account.getLastName() : "null")
                + "}";
    }


    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Account implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("first_name")
        private String firstName;
        @JsonProperty("last_name")
        private String lastName;
        @JsonProperty("middle_name")
        private String middleName;
        @JsonProperty("birth_date")
        private Date birthDate;
        @JsonProperty("gender_code")
        private String genderCode;
        @JsonProperty("country_code")
        private String countryCode;
        @JsonProperty("country_name")
        private String countryName;
        @JsonProperty("civil_status_code")
        private String civilStatusCode;
        @JsonProperty("passport_number")
        private String passportNumber;
        @JsonProperty("registration_date")
        private String registrationDate;
        @JsonProperty("creation_application")
        private String creationApplication;
        @JsonProperty("correlation_id")
        private String correlation_id;

        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getFirstName() { return firstName; }
        public void setFirstName(String firstName) { this.firstName = firstName; }
        public String getLastName() { return lastName; }
        public void setLastName(String lastName) { this.lastName = lastName; }
        public String getMiddleName() { return middleName; }
        public void setMiddleName(String middleName) { this.middleName = middleName; }
        public Date getBirthDate() { return birthDate; }
        public void setBirthDate(Date birthDate) { this.birthDate = birthDate; }
        public String getGenderCode() { return genderCode; }
        public void setGenderCode(String genderCode) { this.genderCode = genderCode; }
        public String getCountryCode() { return countryCode; }
        public void setCountryCode(String countryCode) { this.countryCode = countryCode; }
        public String getCountryName() { return countryName; }
        public void setCountryName(String countryName) { this.countryName = countryName; }
        public String getCivilStatusCode() { return civilStatusCode; }
        public void setCivilStatusCode(String civilStatusCode) { this.civilStatusCode = civilStatusCode; }
        public String getPassportNumber() { return passportNumber; }
        public void setPassportNumber(String passportNumber) { this.passportNumber = passportNumber; }
        public String getRegistrationDate() { return registrationDate; }
        public void setRegistrationDate(String registrationDate) { this.registrationDate = registrationDate; }
        public String getCreationApplication() { return creationApplication; }
        public void setCreationApplication(String creationApplication) { this.creationApplication = creationApplication; }

        public String getCorrelation_id() { return correlation_id; }
        public void setCorrelation_id(String correlation_id) { this.correlation_id = correlation_id; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Loyalty implements Serializable {
        @JsonProperty("identifier_id")
        private String identifierId;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("status_code")
        private String statusCode;


        public String getIdentifierId() { return identifierId; }
        public void setIdentifierId(String identifierId) { this.identifierId = identifierId; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getStatusCode() { return statusCode; }
        public void setStatusCode(String statusCode) { this.statusCode = statusCode; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AddressChannel implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("address_type")
        private String addressType;
        @JsonProperty("option_channel")
        private Boolean optionChannel;
        @JsonProperty("address_street")
        private String addressStreet;
        @JsonProperty("address_zip_code")
        private String addressZipCode;
        @JsonProperty("address_city")
        private String addressCity;
        @JsonProperty("country_code")
        private String countryCode;
        @JsonProperty("geo_coordinates_x_value")
        private BigDecimal geoCoordinatesXValue;
        @JsonProperty("geo_coordinates_y_value")
        private BigDecimal geoCoordinatesYValue;
        @JsonProperty("correlation_id")
        private String correlation_id;

        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getAddressType() { return addressType; }
        public void setAddressType(String addressType) { this.addressType = addressType; }
        public Boolean getOptionChannel() { return optionChannel; }
        public void setOptionChannel(Boolean optionChannel) { this.optionChannel = optionChannel; }
        public String getAddressStreet() { return addressStreet; }
        public void setAddressStreet(String addressStreet) { this.addressStreet = addressStreet; }
        public String getAddressZipCode() { return addressZipCode; }
        public void setAddressZipCode(String addressZipCode) { this.addressZipCode = addressZipCode; }
        public String getAddressCity() { return addressCity; }
        public void setAddressCity(String addressCity) { this.addressCity = addressCity; }
        public String getCountryCode() { return countryCode; }
        public void setCountryCode(String countryCode) { this.countryCode = countryCode; }
        public BigDecimal getGeoCoordinatesXValue() { return geoCoordinatesXValue; }
        public void setGeoCoordinatesXValue(BigDecimal geoCoordinatesXValue) { this.geoCoordinatesXValue = geoCoordinatesXValue; }
        public BigDecimal getGeoCoordinatesYValue() { return geoCoordinatesYValue; }
        public void setGeoCoordinatesYValue(BigDecimal geoCoordinatesYValue) { this.geoCoordinatesYValue = geoCoordinatesYValue; }
//        public String getCreatedAt() { return createdAt; }
//        public void setCreatedAt(String createdAt) { this.createdAt = createdAt; }
//        public String getUpdatedAt() { return updatedAt; }
//        public void setUpdatedAt(String updatedAt) { this.updatedAt = updatedAt; }
        public String getCorrelation_id() { return correlation_id; }
        public void setCorrelation_id(String correlation_id) { this.correlation_id = correlation_id; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContactChannel implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("contact_type")
        private String contactType;
        private String value;
        @JsonProperty("flag_main_type")
        private Boolean flagMainType;
        @JsonProperty("preferred_channel")
        private Boolean preferredChannel;
        @JsonProperty("option_channel")
        private Boolean optionChannel;
        @JsonProperty("flag_valid")
        private Boolean flagValid;

        @JsonProperty("correlation_id")
        private String correlation_id;

        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getContactType() { return contactType; }
        public void setContactType(String contactType) { this.contactType = contactType; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public Boolean getFlagMainType() { return flagMainType; }
        public void setFlagMainType(Boolean flagMainType) { this.flagMainType = flagMainType; }
        public Boolean getPreferredChannel() { return preferredChannel; }
        public void setPreferredChannel(Boolean preferredChannel) { this.preferredChannel = preferredChannel; }
        public Boolean getOptionChannel() { return optionChannel; }
        public void setOptionChannel(Boolean optionChannel) { this.optionChannel = optionChannel; }
        public Boolean getFlagValid() { return flagValid; }
        public void setFlagValid(Boolean flagValid) { this.flagValid = flagValid; }
        public String getCorrelation_id() { return correlation_id; }
        public void setCorrelation_id(String correlation_id) { this.correlation_id = correlation_id; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CommunicationSubscription implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("communication_code")
        private String communicationCode;
        private String value;
        @JsonProperty("date_of_subscription")
        private String dateOfSubscription;
        @JsonProperty("date_of_unsubscription")
        private String dateOfUnsubscription;
        @JsonProperty("reason_of_unsubscription")
        private String reasonOfUnsubscription;

        @JsonProperty("correlation_id")
        private String correlation_id;

        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getCommunicationCode() { return communicationCode; }
        public void setCommunicationCode(String communicationCode) { this.communicationCode = communicationCode; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public String getDateOfSubscription() { return dateOfSubscription; }
        public void setDateOfSubscription(String dateOfSubscription) { this.dateOfSubscription = dateOfSubscription; }
        public String getDateOfUnsubscription() { return dateOfUnsubscription; }
        public void setDateOfUnsubscription(String dateOfUnsubscription) { this.dateOfUnsubscription = dateOfUnsubscription; }
        public String getReasonOfUnsubscription() { return reasonOfUnsubscription; }
        public void setReasonOfUnsubscription(String reasonOfUnsubscription) { this.reasonOfUnsubscription = reasonOfUnsubscription; }
        public String getCorrelation_id() { return correlation_id; }
        public void setCorrelation_id(String correlation_id) { this.correlation_id = correlation_id; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DigitalAccess implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        private String username;
        @JsonProperty("email_user")
        private String emailUser;
        @JsonProperty("is_active")
        private Boolean isActive;
        @JsonProperty("last_login_at")
        private String lastLoginAt;
        @JsonProperty("portal_user_confirmation_at")
        private String portalUserConfirmationAt;


        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getEmailUser() { return emailUser; }
        public void setEmailUser(String emailUser) { this.emailUser = emailUser; }
        public Boolean getIsActive() { return isActive; }
        public void setIsActive(Boolean isActive) { this.isActive = isActive; }
        public String getLastLoginAt() { return lastLoginAt; }
        public void setLastLoginAt(String lastLoginAt) { this.lastLoginAt = lastLoginAt; }
        public String getPortalUserConfirmationAt() { return portalUserConfirmationAt; }
        public void setPortalUserConfirmationAt(String portalUserConfirmationAt) { this.portalUserConfirmationAt = portalUserConfirmationAt; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AccountIndicator implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        private String type;
        @JsonProperty("is_active")
        private Boolean isActive;


        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getType() { return type; }
        public void setType(String type) { this.type = type; }
        public Boolean getIsActive() { return isActive; }
        public void setIsActive(Boolean isActive) { this.isActive = isActive; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Nationality implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("country_code")
        private String countryCode;
        @JsonProperty("correlation_id")
        private String correlation_id;

        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getCountryCode() { return countryCode; }
        public void setCountryCode(String countryCode) { this.countryCode = countryCode; }
        public String getCorrelation_id() { return correlation_id; }
        public void setCorrelation_id(String correlation_id) { this.correlation_id = correlation_id; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Language implements Serializable {
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("language_level")
        private String languageLevel;
        @JsonProperty("language_code")
        private String languageCode;
        @JsonProperty("correlation_id")
        private String correlation_id;

        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getLanguageCode() { return languageCode; }
        public void setLanguageCode(String languageCode) { this.languageCode = languageCode; }
        public String getLanguageLevel() { return languageLevel; }
        public void setLanguageLevel(String languageLevel) { this.languageLevel = languageLevel; }
        public String getCorrelation_id() { return correlation_id; }
        public void setCorrelation_id(String correlation_id) { this.correlation_id = correlation_id; }
    }
}
