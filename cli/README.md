# MSB-Java CLI

Microservice bus - Java CLI 

## Build:
```
mvn clean install
```
## Run:

without options to show help:
```
java -jar msb-java-cli-*.jar  
```
with topic:
```
java -jar msb-java-cli-*.jar --topic pingpong:namespace
```
with external config:
```
java -Dconfig.file=./application.conf -jar msb-java-cli-*.jar --topic pingpong:namespace
```
## Usage

Listens to a topic on the bus and prints JSON to stdout. By default it will also listen for response topics detected on messages, and JSON is pretty-printed. For [Newline-delimited JSON](http://en.wikipedia.org/wiki/Line_Delimited_JSON) compatibility, specify `-p false`.

Options:
- **--topic** or **-t** (By adding '.fanout' or '.topic' to topic name you can specify a type of the exchange, Default:'.fanout', Example: --topic pingpong:namespace.fanout)
- **--follow** or **-f** listen for following topics, empty to disable (Default: response, ack)
- **--pretty** or **-p** set to false to use as a newline-delimited json stream, (Default: true)
- **-Dconfig.file=<path>** specify external config file if needed
