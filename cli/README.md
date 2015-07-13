# MSB-Java CLI

Microservice bus - Java CLI

## Build:
```
mvn clean compile assembly:single
```

## Run:
```
java -jar <path to jar>
```

## Usage

Listens to a topic on the bus and prints JSON to stdout. By default it will also listen for response topics detected on messages, and JSON is pretty-printed. For [Newline-delimited JSON](http://en.wikipedia.org/wiki/Line_Delimited_JSON) compatibility, specify `-p false`.

Options:
- **--topic** or **-t**
- **--follow** or **-f** listen for following topics, empty to disable (Default: response, ack)
- **--pretty** or **-p** set to false to use as a newline-delimited json stream, (Default: true)
