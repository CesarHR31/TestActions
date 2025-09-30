package applicationinsights;

import com.microsoft.applicationinsights.TelemetryClient;
import com.microsoft.applicationinsights.TelemetryConfiguration;


public class ApplicationInsightsTelemetry {

    public final TelemetryClient telemetryClient;

    public ApplicationInsightsTelemetry(String instrumentationKey) {
        TelemetryConfiguration telemetryConfiguration = TelemetryConfiguration.createDefault();
        telemetryConfiguration.setInstrumentationKey(instrumentationKey);
        telemetryClient = new TelemetryClient(telemetryConfiguration);
    }
}
