package applicationinsights;

import org.apache.logging.log4j.message.Message;
import sat.diot.entities.SatInsightsEntity;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;

public class SatInsights implements Message {
    private final String layer;
    private final String application;
    private final String environment;
    private final String applicationCode;
    private final String correlationId;
    private final String statusCode;
    private final Timestamp creationTime;
    private final Integer severity;
    private final String source;
    private final String origin;
    private final String data;

    public SatInsights(SatInsightsEntity customEntitySatInsights){
        layer = customEntitySatInsights.layer();
        application = customEntitySatInsights.application();
        environment = customEntitySatInsights.environment();
        applicationCode = customEntitySatInsights.applicationCode();
        correlationId = customEntitySatInsights.correlationId();
        statusCode = customEntitySatInsights.statusCode();
        creationTime = customEntitySatInsights.creationTime();
        severity = customEntitySatInsights.severity();
        source = customEntitySatInsights.source();
        origin = customEntitySatInsights.origin();
        data = customEntitySatInsights.data();
    }

    public Map<String, String> toHashMap() {
        Map<String, String> hashMap = new HashMap<>();

        hashMap.put("Capa", this.layer);
        hashMap.put("Aplicacion", this.application);
        hashMap.put("Ambiente", this.environment);
        hashMap.put("CodigoAplicacion", this.applicationCode);
        hashMap.put("IdCorrelacion", this.correlationId);
        hashMap.put("CodigoEstatus", this.statusCode);
        hashMap.put("TiempoCreacion", this.creationTime.toString());
        hashMap.put("Severidad", this.severity.toString());
        hashMap.put("Fuente", this.source);
        hashMap.put("Origen", this.origin);
        hashMap.put("Datos", this.data);
        return hashMap;
    }

    @Override
    public String getFormattedMessage() {
        return this.data;
    }

    @Override
    public String getFormat() {
        return "";
    }

    @Override
    public Object[] getParameters() {
        return new Object[0];
    }

    @Override
    public Throwable getThrowable() {
        return null;
    }
}
