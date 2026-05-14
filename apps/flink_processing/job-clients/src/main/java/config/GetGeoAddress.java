//package config;
//
//import com.fasterxml.jackson.databind.JsonNode;
//import com.fasterxml.jackson.databind.ObjectMapper;
//import dto.Client;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.functions.RichMapFunction;
//import org.apache.flink.configuration.Configuration;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import org.slf4j.MDC;
//
//public class GetGeoAddress extends RichMapFunction<Client.AddressChannel, Client.AddressChannel> {
//    private transient GeoCode geoCode;
//    private transient ObjectMapper objectMapper;
//    private static final Logger LOG = LoggerFactory.getLogger(GetGeoAddress.class);
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        this.geoCode = new GeoCode();
//        this.objectMapper = new ObjectMapper();
//    }
//    @Override
//    public Client.AddressChannel map(Client.AddressChannel addressChannel) throws Exception {
//        try {
//            MDC.put("service", "flink-clients");
//            MDC.put("correlation_id", addressChannel.getCorrelation_id());
//            String response = geoCode.getCoord(addressChannel.getAddressStreet());
//            JsonNode jsonNode = objectMapper.readTree(response);
//            LOG.info("Geocode response: {}", jsonNode);
//            if (jsonNode.isArray() && !jsonNode.isEmpty()) {
//                addressChannel.setLatitude(jsonNode.get(0).get("lat").asDouble());
//                addressChannel.setLongitude(jsonNode.get(0).get("lon").asDouble());
//            }else {
//                LOG.warn("No geocode found for address {}", addressChannel.getAddressStreet());
//                addressChannel.setLatitude(null);
//                addressChannel.setLongitude(null);
//            }
//        } catch (Exception e) {
//            LOG.error("Error getting geo coordinates for address: {}", addressChannel.getAddressStreet(), e);
//            addressChannel.setLatitude(null);
//            addressChannel.setLongitude(null);
//        }return addressChannel;
//    }
//
//
//}
