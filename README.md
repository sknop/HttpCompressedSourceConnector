# HttpCompressedSourceConnector
Kafka Connect Source Connector for pulling compressed file from HTTP endpoint

Connect to an endpoint and regularly download a GZIP-compressed file.
The endpoint is protected via Basic Authentication (username:password). 
The file is broken down into single lines and then written into a single topic.

The connector is paged, that is, a maximum number of lines will be written in a single batch. The batch size is configurable and set to 1000 lines by default.
The end of the downloaded file is expected to end with 

  {"EOF":true} 
  
but files that do not end this way will be handled gracefully. 

Once the end of the file is reached, the connection to the HTTP endpoint is closed. 
Further calls will check the timestamp embedded in the HTTP Header ("lastModified"). 
If the timestamp matches the previous timestamp, the task will wait for a configurable amount of milliseconds before returning to the connect Worker.

The connector is designed to download a large compressed file (say, 100,000 or more lines) that changes, for example, once a day.

The following configuration properties are defined:

- http.url: URL from which to read the compressed file (required)
- http.user: Username for Authentication (required)
- http.password: Password for Authentication (sensitive - required)
- topic: The topic to which to write to (required)
- page.size.lines: Number of lines poll returns each go(optional, default 1000)
- task.pause.ms: Task pause before returning if nothing to do (optional, default 10,000 ms)

