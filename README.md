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
- POST /<object_id>/files/<file_name>
    - url params: message, username, useraddress - these get added to OCFL version info
        - checksum, checksumtype - verify that the received data matches what user sent
        - location - path to file to add (eg. file:///tmp/file.txt)
    - body: the contents to be stored in <file_name> in OCFL (if location param wasn't used)
    - fails if <file_name> is already in the OCFL object
- PUT /<object_id>/files/<file_name>
    - url params: message, username, useraddress - these get added to OCFL version info
        - checksum, checksumtype - verify that the received data matches what user sent
        - location - path to file
    - body: the contents to be stored in <file_name> in OCFL
    - fails if <file_name> isn't already in the OCFL

Development
-----------
- test: mvn clean test
- generate executable jar: mvn clean verify
  - creates target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar, which you can run with "java -jar target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar"
