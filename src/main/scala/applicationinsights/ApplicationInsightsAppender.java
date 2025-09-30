package applicationinsights;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Property;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.message.Message;
import sat.diot.comunes.CatalogoProcesoNPSI;
import sat.diot.comunes.Util;
import sat.diot.comunes.config.ConfigurationProvider;
import sat.diot.entities.SatInsightsEntity;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@Plugin(name = "ApplicationInsightsAppender", category = "Core", elementType = "appender", printObject = true)
public final class ApplicationInsightsAppender extends AbstractAppender {
    private static final Logger logger = LogManager.getLogger(ApplicationInsightsAppender.class);

    private static final String INSTRUMENTATIONKEY = ConfigurationProvider.applicationInsightsKey();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final Lock readLock = rwLock.readLock();
    private final ApplicationInsightsTelemetry applicationInsightsTelemetry = new ApplicationInsightsTelemetry(INSTRUMENTATIONKEY);

    private ApplicationInsightsAppender(String name, Filter filter, Layout<? extends Serializable> layout) {
        super(name, filter, layout, true, Property.EMPTY_ARRAY);
    }

    @Override
    public void append(LogEvent event) {
        readLock.lock();
        try {
            Message message = event.getMessage();

            Integer severidad;
            Level eventLevel = event.getLevel();

            severidad = obtenerSeveridad(eventLevel);

            String source = event.getSource().toString();

            if (message instanceof SatInsights) {
                SatInsights customLogMessage = (SatInsights) message;
                Map<String, String> hashMap = customLogMessage.toHashMap();
                applicationInsightsTelemetry.telemetryClient.trackEvent(event.getLevel().toString(), hashMap, null);
            } else {

                SatInsights test =
                        new SatInsights(new SatInsightsEntity(
                                ConfigurationProvider.identificadorCapa(),
                                ConfigurationProvider.identificadorAplicacion(),
                                ConfigurationProvider.identificadorAmbiente(),
                                ConfigurationProvider.identificadorCodigoAplicacion(),
                                java.util.UUID.randomUUID().toString(),
                                ConfigurationProvider.firstDigitApplicationCode() +
                                        ConfigurationProvider.idNumericDataProcess() +
                                        severidad.toString() +
                                        String.format("%04.0f", Double.valueOf(String.valueOf(CatalogoProcesoNPSI.ProcesoDesdobleInformacion()))),
                                Util.obtenerTimestamp("America/Mexico_City"),
                                severidad,
                                source,
                                ConfigurationProvider.identificadorOrigen(),
                                message.getFormattedMessage())
                        );

                applicationInsightsTelemetry
                        .telemetryClient
                        .trackEvent(event.getLevel().toString(), test.toHashMap(), null);
            }

            applicationInsightsTelemetry
                    .telemetryClient
                    .flush();

        } catch (Exception ex) {
            logger.error("Error al enviar evento a Application Insights: {}", ex.getMessage());
        } finally {
            readLock.unlock();
        }
    }

    private Integer obtenerSeveridad(Level eventLevel) {
        int severidad = 0;
        switch (eventLevel.getStandardLevel().intLevel()) {
            case 400: //INFO
                severidad = 1;
                break;
            case 300: //WARN
                severidad = 2;
                break;
            case 200: // ERROR
                severidad = 3;
                break;
            case 100: // FATAL
                severidad = 4;
                break;
            default: //DEBUG
                break;
        }
        return severidad;
    }

    @PluginFactory
    public static ApplicationInsightsAppender createAppender(
            @PluginAttribute("name") String name,
            @PluginElement("Layout") Layout<? extends Serializable> layout,
            @PluginElement("Filter") final Filter filter) {
        return new ApplicationInsightsAppender(name, filter, layout);
    }
}
