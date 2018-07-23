# Streaming S3 Multipart Upload

Java POC for uploading a file from an InputStream to S3 without knowing
the size of the file. There is a single endpoint to allow uploads of
varying files that are currently generated or already exist on the server.

Build with: `mvn package`

Run with: `java -jar streaming-poc-1.0-SNAPSHOT-shaded.jar`

Example requests:

`curl localhost:8080/upload/s3?fileName=/tmp/andy/1_gb`

`curl localhost:8080/upload/s3?fileSizeMb=10`

