OCFL-JAVA-HTTP
==============

Lightweight HTTP layer for an OCFL repository.

Development
-----------
- compile: mvn clean compile (artifacts go into target/)
- test: mvn clean test
- run: mvn clean exec:java -Dexec.mainClass=edu.brown.library.repository.OcflHttp
- generate executable jar: mvn clean verify
  - creates target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar, which you can run with "java -jar target/ocfl-java-http-0.1-SNAPSHOT-launcher.jar"
