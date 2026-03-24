package Dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.sql.Date;
import java.util.List;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Client implements Serializable {

    private Account account;
    private Loyalty loyalty;

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



    public Account getAccount() { return account; }
    public void setAccount(Account account) { this.account = account; }

    public Loyalty getLoyalty() { return loyalty; }
    public void setLoyalty(Loyalty loyalty) { this.loyalty = loyalty; }

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

    @Override
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
        @JsonProperty("civil_status")
        private String civilStatus;
        @JsonProperty("passport_number")
        private String passportNumber;
        @JsonProperty("registration_date")
        private Date registrationDate;
        @JsonProperty("creation_application")
        private String creationApplication;

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
        public String getCivilStatus() { return civilStatus; }
        public void setCivilStatus(String civilStatus) { this.civilStatus = civilStatus; }
        public String getPassportNumber() { return passportNumber; }
        public void setPassportNumber(String passportNumber) { this.passportNumber = passportNumber; }
        public Date getRegistrationDate() { return registrationDate; }
        public void setRegistrationDate(Date registrationDate) { this.registrationDate = registrationDate; }
        public String getCreationApplication() { return creationApplication; }
        public void setCreationApplication(String creationApplication) { this.creationApplication = creationApplication; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Loyalty implements Serializable {
        @JsonProperty("identifier_id")
        private String identifierId;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("loyalty_status")
        private String loyaltyStatus;
        @JsonProperty("start_date")
        private String startDate;
        @JsonProperty("end_date")
        private String endDate;

        public String getIdentifierId() { return identifierId; }
        public void setIdentifierId(String identifierId) { this.identifierId = identifierId; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getLoyaltyStatus() { return loyaltyStatus; }
        public void setLoyaltyStatus(String loyaltyStatus) { this.loyaltyStatus = loyaltyStatus; }
        public String getStartDate() { return startDate; }
        public void setStartDate(String startDate) { this.startDate = startDate; }
        public String getEndDate() { return endDate; }
        public void setEndDate(String endDate) { this.endDate = endDate; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AddressChannel implements Serializable {
        @JsonProperty("channel_id")
        private String channelId;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("channel_type")
        private String channelType;
        private String value;
        @JsonProperty("flag_main_type")
        private Boolean flagMainType;
        @JsonProperty("preferred_channel")
        private Boolean preferredChannel;
        @JsonProperty("address_address")
        private String addressAddress;
        @JsonProperty("address_zip_code")
        private String addressZipCode;
        @JsonProperty("address_code")
        private String addressCode;
        @JsonProperty("address_city")
        private String addressCity;
        @JsonProperty("option_channel")
        private String optionChannel;
        @JsonProperty("flag_valid")
        private Boolean flagValid;
        @JsonProperty("created_date")
        private String createdDate;
        @JsonProperty("last_modified_date")
        private String lastModifiedDate;
        @JsonProperty("is_deleted")
        private Boolean isDeleted;

        public String getChannelId() { return channelId; }
        public void setChannelId(String channelId) { this.channelId = channelId; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getChannelType() { return channelType; }
        public void setChannelType(String channelType) { this.channelType = channelType; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public Boolean getFlagMainType() { return flagMainType; }
        public void setFlagMainType(Boolean flagMainType) { this.flagMainType = flagMainType; }
        public Boolean getPreferredChannel() { return preferredChannel; }
        public void setPreferredChannel(Boolean preferredChannel) { this.preferredChannel = preferredChannel; }
        public String getAddressAddress() { return addressAddress; }
        public void setAddressAddress(String addressAddress) { this.addressAddress = addressAddress; }
        public String getAddressZipCode() { return addressZipCode; }
        public void setAddressZipCode(String addressZipCode) { this.addressZipCode = addressZipCode; }
        public String getAddressCode() { return addressCode; }
        public void setAddressCode(String addressCode) { this.addressCode = addressCode; }
        public String getAddressCity() { return addressCity; }
        public void setAddressCity(String addressCity) { this.addressCity = addressCity; }
        public String getOptionChannel() { return optionChannel; }
        public void setOptionChannel(String optionChannel) { this.optionChannel = optionChannel; }
        public Boolean getFlagValid() { return flagValid; }
        public void setFlagValid(Boolean flagValid) { this.flagValid = flagValid; }
        public String getCreatedDate() { return createdDate; }
        public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
        public String getLastModifiedDate() { return lastModifiedDate; }
        public void setLastModifiedDate(String lastModifiedDate) { this.lastModifiedDate = lastModifiedDate; }
        public Boolean getIsDeleted() { return isDeleted; }
        public void setIsDeleted(Boolean isDeleted) { this.isDeleted = isDeleted; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class ContactChannel implements Serializable {
        @JsonProperty("channel_id")
        private String channelId;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("channel_type")
        private String channelType;
        private String value;
        @JsonProperty("flag_main_type")
        private Boolean flagMainType;
        @JsonProperty("preferred_channel")
        private Boolean preferredChannel;
        @JsonProperty("option_channel")
        private String optionChannel;
        @JsonProperty("flag_valid")
        private Boolean flagValid;
        @JsonProperty("created_date")
        private String createdDate;
        @JsonProperty("last_modified_date")
        private String lastModifiedDate;
        @JsonProperty("is_deleted")
        private Boolean isDeleted;

        public String getChannelId() { return channelId; }
        public void setChannelId(String channelId) { this.channelId = channelId; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getChannelType() { return channelType; }
        public void setChannelType(String channelType) { this.channelType = channelType; }
        public String getValue() { return value; }
        public void setValue(String value) { this.value = value; }
        public Boolean getFlagMainType() { return flagMainType; }
        public void setFlagMainType(Boolean flagMainType) { this.flagMainType = flagMainType; }
        public Boolean getPreferredChannel() { return preferredChannel; }
        public void setPreferredChannel(Boolean preferredChannel) { this.preferredChannel = preferredChannel; }
        public String getOptionChannel() { return optionChannel; }
        public void setOptionChannel(String optionChannel) { this.optionChannel = optionChannel; }
        public Boolean getFlagValid() { return flagValid; }
        public void setFlagValid(Boolean flagValid) { this.flagValid = flagValid; }
        public String getCreatedDate() { return createdDate; }
        public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
        public String getLastModifiedDate() { return lastModifiedDate; }
        public void setLastModifiedDate(String lastModifiedDate) { this.lastModifiedDate = lastModifiedDate; }
        public Boolean getIsDeleted() { return isDeleted; }
        public void setIsDeleted(Boolean isDeleted) { this.isDeleted = isDeleted; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class CommunicationSubscription implements Serializable {
        @JsonProperty("communication_id")
        private String communicationId;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("community_code")
        private String communityCode;
        @JsonProperty("community_code_value")
        private String communityCodeValue;
        @JsonProperty("date_of_subscription")
        private String dateOfSubscription;
        @JsonProperty("date_of_unsubscription")
        private String dateOfUnsubscription;
        @JsonProperty("reason_of_unsubscription")
        private String reasonOfUnsubscription;
        @JsonProperty("last_modified_date")
        private String lastModifiedDate;

        public String getCommunicationId() { return communicationId; }
        public void setCommunicationId(String communicationId) { this.communicationId = communicationId; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getCommunityCode() { return communityCode; }
        public void setCommunityCode(String communityCode) { this.communityCode = communityCode; }
        public String getCommunityCodeValue() { return communityCodeValue; }
        public void setCommunityCodeValue(String communityCodeValue) { this.communityCodeValue = communityCodeValue; }
        public String getDateOfSubscription() { return dateOfSubscription; }
        public void setDateOfSubscription(String dateOfSubscription) { this.dateOfSubscription = dateOfSubscription; }
        public String getDateOfUnsubscription() { return dateOfUnsubscription; }
        public void setDateOfUnsubscription(String dateOfUnsubscription) { this.dateOfUnsubscription = dateOfUnsubscription; }
        public String getReasonOfUnsubscription() { return reasonOfUnsubscription; }
        public void setReasonOfUnsubscription(String reasonOfUnsubscription) { this.reasonOfUnsubscription = reasonOfUnsubscription; }
        public String getLastModifiedDate() { return lastModifiedDate; }
        public void setLastModifiedDate(String lastModifiedDate) { this.lastModifiedDate = lastModifiedDate; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class DigitalAccess implements Serializable {
        private String id;
        @JsonProperty("person_id")
        private String personId;
        private String username;
        @JsonProperty("email_user")
        private String emailUser;
        @JsonProperty("is_active")
        private Boolean isActive;
        @JsonProperty("last_login_date")
        private String lastLoginDate;
        @JsonProperty("created_date")
        private String createdDate;
        @JsonProperty("portal_user_confirmation_date")
        private String portalUserConfirmationDate;
        @JsonProperty("preferred_delivery_method")
        private String preferredDeliveryMethod;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getUsername() { return username; }
        public void setUsername(String username) { this.username = username; }
        public String getEmailUser() { return emailUser; }
        public void setEmailUser(String emailUser) { this.emailUser = emailUser; }
        public Boolean getIsActive() { return isActive; }
        public void setIsActive(Boolean isActive) { this.isActive = isActive; }
        public String getLastLoginDate() { return lastLoginDate; }
        public void setLastLoginDate(String lastLoginDate) { this.lastLoginDate = lastLoginDate; }
        public String getCreatedDate() { return createdDate; }
        public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
        public String getPortalUserConfirmationDate() { return portalUserConfirmationDate; }
        public void setPortalUserConfirmationDate(String portalUserConfirmationDate) { this.portalUserConfirmationDate = portalUserConfirmationDate; }
        public String getPreferredDeliveryMethod() { return preferredDeliveryMethod; }
        public void setPreferredDeliveryMethod(String preferredDeliveryMethod) { this.preferredDeliveryMethod = preferredDeliveryMethod; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class AccountIndicator implements Serializable {
        private String id;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("type_account_indicator")
        private String typeAccountIndicator;
        @JsonProperty("value_account_indicator")
        private String valueAccountIndicator;
        @JsonProperty("last_modified_date")
        private String lastModifiedDate;
        @JsonProperty("is_deleted")
        private Boolean isDeleted;
        @JsonProperty("created_date")
        private String createdDate;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getTypeAccountIndicator() { return typeAccountIndicator; }
        public void setTypeAccountIndicator(String typeAccountIndicator) { this.typeAccountIndicator = typeAccountIndicator; }
        public String getValueAccountIndicator() { return valueAccountIndicator; }
        public void setValueAccountIndicator(String valueAccountIndicator) { this.valueAccountIndicator = valueAccountIndicator; }
        public String getLastModifiedDate() { return lastModifiedDate; }
        public void setLastModifiedDate(String lastModifiedDate) { this.lastModifiedDate = lastModifiedDate; }
        public Boolean getIsDeleted() { return isDeleted; }
        public void setIsDeleted(Boolean isDeleted) { this.isDeleted = isDeleted; }
        public String getCreatedDate() { return createdDate; }
        public void setCreatedDate(String createdDate) { this.createdDate = createdDate; }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class Language implements Serializable {
        private String id;
        @JsonProperty("person_id")
        private String personId;
        @JsonProperty("language_code")
        private String languageCode;
        @JsonProperty("language_name")
        private String languageName;
        @JsonProperty("language_level")
        private String languageLevel;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getPersonId() { return personId; }
        public void setPersonId(String personId) { this.personId = personId; }
        public String getLanguageCode() { return languageCode; }
        public void setLanguageCode(String languageCode) { this.languageCode = languageCode; }
        public String getLanguageName() { return languageName; }
        public void setLanguageName(String languageName) { this.languageName = languageName; }
        public String getLanguageLevel() { return languageLevel; }
        public void setLanguageLevel(String languageLevel) { this.languageLevel = languageLevel; }
    }
}
