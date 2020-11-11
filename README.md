OCFL-JAVA-HTTP
==============

Lightweight HTTP layer for an OCFL repository.

API
---
- GET /
    - returns {"OCFL ROOT": ...} as JSON
- GET /<object_id>/files
    - returns {"files": {"file1": {}}} as JSON
- GET /<object_id>/files/<file_name>/content returns file contents
    - use "Range" header to request partial file contents
- POST /<object_id>/files
    - url params: message, username, useraddress - these get added to OCFL version info
    - body: multipart/form-data files to be added to the object
    - fails if the object already exists
- PUT /<object_id>/files
    - url params: message, username, useraddress - these get added to OCFL version info
    - body: multipart/form-data files to be updated in the object
    - fails if the object doesn't exist or the files already exist
    - adding the updateExisting=yes URL parameter allows updating existing files

Development
-----------
- test: mvn clean test
- generate executable jar: mvn clean verify
  - creates target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar, which you can run with "java -jar target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar"
