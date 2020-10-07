OCFL-JAVA-HTTP
==============

Lightweight HTTP layer for an OCFL repository.

API
---
- GET /
    - returns {"OCFL ROOT": ...} as JSON
- GET /<object_id>/files
    - returns {"files": {"file1": {}}} as JSON
- GET /<object_id>/<file_name>/content returns file contents
    - use "Range" header to request partial file contents
- POST /<object_id>/<file_name>
    - url params: message, username, useraddress - these get added to OCFL version info
    - body: the contents to be stored in <file_name> in OCFL
    - fails if <file_name> is already in the OCFL object
- PUT /<object_id>/<file_name>
    - url params: same as for POST
    - body: same as for POST
    - fails if <file_name> isn't already in the OCFL

Development
-----------
- test: mvn clean test
- generate executable jar: mvn clean verify
  - creates target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar, which you can run with "java -jar target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar"
