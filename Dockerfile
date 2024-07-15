FROM confluentinc/cp-kafka-connect-base:7.6.1

# Create the plugin directory
RUN mkdir -p /usr/share/java/pluginsnew

# Copy the JAR file to the plugin directory
COPY target/custom-s3-sink-connector-0.0.1-SNAPSHOT.jar /usr/share/java/pluginsnew/

# Set the plugin path environment variable
ENV CONNECT_PLUGIN_PATH="/usr/share/java/pluginsnew,/usr/share/confluent-hub-components"
