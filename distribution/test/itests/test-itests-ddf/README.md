
### Integration tests that require Postgres docker image

#### NOTE: Make sure Docker is installed and configured in your environment first

### Prerequisite
`postgres` image must first be installed. This can be done from ${DDF_ROOT}/distribution/docker/postgres via `mvn -Pdocker install`

### Running integration tests
Execute: `mvn -Pdocker verify`

##### NOTE: This will run all integration tests (even those that do not require Postgres docker image)

### Running only integration tests which require docker
You may utilize the `skipDefaultDDFItests` flag to disable the default execution configuration of `maven-failsafe-plugin`,
i.e. `mvn -Pdocker verify -DskipDefaultDDFItests`. This will result in only the docker-requiring integration tests being executed.