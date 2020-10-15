#!/bin/sh

mvn clean install -DskipTests -Dcheckstyle.skip -Drat.skip=true -Dscalastyle.skip=true
