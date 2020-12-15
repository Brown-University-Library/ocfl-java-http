OCFL-JAVA-HTTP
==============

Lightweight HTTP layer for an OCFL repository.

API
---
- GET /
    - returns {"OCFL ROOT": ...} as JSON
- GET /<object_id>/files
    - add includeDeleted=true URL param to request all files, not just currently active ones
    - add fields=state,size,mimetype,checksum,lastModified URL param to request 1 or more pieces of information about the files
        - state: "A" for currently active files, "D" for files that have been removed in a later OCFL version
        - size: # of bytes
        - checksum: adds "checksum": <sha512 hash> and "checksumType": "SHA-512"
        - lastModified: UTC timestamp, eg. 2020-11-25T20:30:43.73776Z
    - add objectTimestamps=true URL param to request object created/lastModified timestamps
    - returns {"object": {"created": "2020-11-20T20:30:43.73776Z", "lastModified": "2020-11-25T20:30:43.73776Z"}, files": {"file1": {"state": "A"}}} as JSON
    - returns 410 Gone if object has been deleted (ie. if all files have been removed from latest version)
- POST /<object_id>/files
    - url params: message, userName, userAddress - these get added to OCFL version info
    - body: multipart/form-data
        - "params" field - JSON data in the form: {<filename>: {"checksum": <checksum>, "checksumType": "MD5"/"SHA-512"/..., "location": <file URI>}}
        - "files" field - 1 or more files to be added to the object
    - fails if the object already exists
- PUT /<object_id>/files
    - url params: message, userName, userAddress - these get added to OCFL version info
    - body: multipart/form-data (see description in POST section)
    - fails if the object doesn't exist or the files already exist
    - adding the updateExisting=true URL parameter allows updating existing files
- DELETE /<object_id>/files
    - delete object (by removing all the files in a new version - doesn't actually purge any data)
    - returns 204, if successful (or if object was already deleted), 404 if object doesn't exist
- GET /<object_id>/files/<file_name>/content returns file contents
    - use "Range" header to request partial file contents
    - returns 410 Gone if object or file has been deleted
- DELETE /<object_id>/files/<file_name>
    - delete file <file_name> from object
    - returns 204 if successful (or if file was already deleted), 404 if <file_name> doesn't exist

Development
-----------
- test: mvn clean test
- generate executable jar: mvn clean verify
  - creates target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar, which you can run with "java -jar target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar"
