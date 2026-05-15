from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut, GeocoderRateLimited
from time import sleep
from service.logger_config import get_logger

logger = get_logger("service.geocoding_crm_address.address_geocoder", service="geocoding_crm_address")

GEOCODE_TIMEOUT = 10
GEOCODE_MAX_RETRIES = 3
RETRY_DELAY = 4


class AddressGeocoder:
    def __init__(self):
        self.nominatim = Nominatim(user_agent="GeocodingCRMAdress")

    def geocode_address(self, address):
        for attempt in range(1, GEOCODE_MAX_RETRIES + 1):
            try:
                location = self.nominatim.geocode(address, timeout=GEOCODE_TIMEOUT)
                sleep(1.2)
                if not location:
                    return None, None
                return location.latitude, location.longitude
            except (GeocoderUnavailable, GeocoderTimedOut, GeocoderRateLimited) as e:
                logger.warning("Geocoder error, retrying", extra={
                    "class": self.__class__.__name__,
                    "method": "geocode_address",
                    "address": address,
                    "attempt": attempt,
                    "max_retries": GEOCODE_MAX_RETRIES,
                    "error_type": type(e).__name__,
                    "error": str(e),
                })
                sleep(RETRY_DELAY * attempt)

        logger.error("Geocoder failed after all retries", extra={
            "class": self.__class__.__name__,
            "method": "geocode_address",
            "address": address,
            "max_retries": GEOCODE_MAX_RETRIES,
        })
        return None, None
