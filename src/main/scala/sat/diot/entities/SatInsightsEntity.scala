package sat.diot.entities

import java.sql.Timestamp

case class SatInsightsEntity(
                              layer: String,
                              application: String,
                              environment: String,
                              applicationCode: String,
                              correlationId: String,
                              statusCode: String,
                              creationTime: Timestamp,
                              severity: Integer,
                              source: String,
                              origin: String,
                              data: String
                            )