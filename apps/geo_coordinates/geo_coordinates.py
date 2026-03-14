import time

from geopy.geocoders import Nominatim
# from apps.geo_coordinates.exceptions import GeoCoordinatesError
from apps.logger_config import get_logger,correlation_id
import uuid
logger = get_logger(__name__)

class GeoCoordinates:
    def __init__(self,correlation_id:str=None):
        self.correlation_id = correlation_id
        self.geolocator = Nominatim(user_agent="GeoCoordinates")

    def get_coordinates(self, address: str):
        try:
            location = self.geolocator.geocode(address)
            if location:
                return location.latitude, location.longitude
            else:
                logger.warning(f"Could not geocode address: {address}", extra={'class':'GeoCoordinates','method':
                    'get_coordinates',"correlation_id": self.correlation_id})
                return None,None
        except Exception as e:
            logger.error(f"Error geocoding address: {address}",
                        extra={'class':'GeoCoordinates','method':'get_coordinates',"correlation_id": self.correlation_id,'error': str(e)}, exc_info=True)
            raise ValueError(f"Error geocoding address: {address}. Error: {str(e)}")



