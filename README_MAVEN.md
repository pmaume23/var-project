# VaR Project - Maven Build Instructions

## Prerequisites
- Java 8 or higher
- Maven 3.6+
- Scala 2.12
- PostgreSQL with sp500stocks database

## Build Commands

### Clean and compile
```bash
mvn clean compile
```

### Run tests
```bash
mvn test
```

### Package (create JAR)
```bash
mvn package
```

### Run the application
```bash
mvn exec:java -Dexec.mainClass="com.varproject.DQMain"
```

### Or run the packaged JAR
```bash
java -cp target/var-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.varproject.DQMain
```

## Project Structure
```
var-project/
├── pom.xml                          # Maven build configuration
├── src/
│   ├── main/
│   │   ├── scala/
│   │   │   └── com/varproject/
│   │   │       ├── DQMain.scala                    # Main entry point
│   │   │       └── helpers/
│   │   │           ├── DatabaseHelper.scala        # Database operations
│   │   │           └── SparkHelper.scala           # Spark session management
│   │   └── resources/
│   │       └── application.conf                     # Configuration file
│   └── test/
│       └── scala/
└── .env                              # Environment variables (not in git)
```

## Configuration

Database credentials are loaded from `src/main/resources/application.conf` and can be overridden with environment variables in `.env` file.

## Environment Variables (Optional)
Create a `.env` file in the project root:
```
DB_HOST=127.0.0.1
DB_PORT=5432
DB_NAME=sp500stocks
DB_USER=varuser
DB_PASSWORD=varuserpass@01
```
