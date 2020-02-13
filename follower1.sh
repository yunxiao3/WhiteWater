#!/bin/bash
java -cp target/tender2-1.0-SNAPSHOT-jar-with-dependencies.jar   -classpath target/classes io.jiache.main.SeverMain localhost:8201 localhost:8300 localhost:8201,localhost:8202,localhost:8200

