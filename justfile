run:
    ./gradlew run

# auto runs full test suite
build:
    ./gradlew build

# run only certain tests
test_base:
    ./gradlew test --tests "it.unitn.ds.base*"

test_extra:
    ./gradlew test --tests "it.unitn.ds.extra*"

wrap:
    gradle wrapper --gradle-version 9.2.1
