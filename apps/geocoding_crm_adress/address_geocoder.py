from geopy.geocoders import Nominatim
from geopy.exc import GeocoderUnavailable, GeocoderTimedOut, GeocoderRateLimited
from time import sleep
import logging

logger = logging.getLogger(__name__)

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
                sleep(2)
                if not location:
                    return None, None
                return location.latitude, location.longitude
            except (GeocoderUnavailable, GeocoderTimedOut, GeocoderRateLimited) as e:
                logger.warning(
                    "Geocoder error for '%s' (attempt %d/%d): %s",
                    address, attempt, GEOCODE_MAX_RETRIES, e
                )
                sleep(RETRY_DELAY * attempt)

        logger.error("Geocoder failed after %d attempts for '%s'", GEOCODE_MAX_RETRIES, address)
        return None, None
