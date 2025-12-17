# VAR Project - Data Quality & Value at Risk Analysis

A comprehensive Scala/Apache Spark application for financial data quality assessment and Value at Risk (VaR) / Stressed VaR (SVaR) calculations using historical simulation methodology.

## Overview

This project provides a robust pipeline for:
- **Data Quality Assessment**: Multi-dimensional data quality checks including completeness, conformity, validity, and uniqueness validation
- **Risk Calculation**: Value at Risk (VaR) and Stressed Value at Risk (SVaR) computation for portfolio analysis using 252-day rolling windows and crisis period stress testing
- **Data Onboarding**: Python-based ETL for ingesting S&P 500 historical data
- **Analysis**: Interactive Jupyter notebooks for exploratory data analysis

## Technology Stack

- **Language**: Scala 2.12.18
- **Framework**: Apache Spark 3.5.0
- **Build Tool**: Maven 3.x
- **Java**: OpenJDK 17.0.17
- **Testing**: ScalaTest 3.2.17
- **Python**: 3.x (for data onboarding)

## Project Structure

```
var-project/
├── README.md                           # This file
├── pom.xml                            # Maven configuration
├── run.sh                             # Run application with Spark
├── run-standalone.sh                  # Run application in standalone mode
├── data_onboarding/
│   └── onboard_data.py               # Python script to load S&P 500 data
├── EDA/
│   └── eda_var_analysis.ipynb        # Exploratory Data Analysis notebook
├── src/
│   ├── main/
│   │   ├── resources/
│   │   │   └── application.conf       # Application configuration
│   │   └── scala/
│   │       ├── DQMain.scala          # Data Quality pipeline main entry point
│   │       └── com/
│   │           ├── dq/
│   │           │   └── pipeline/
│   │           │       ├── Helpers/
│   │           │       │   ├── DatabaseHelper.scala
│   │           │       │   ├── DQArithmeticAgent.scala
│   │           │       │   ├── LoadSP500Data.scala
│   │           │       │   └── SparkHelper.scala
│   │           │       ├── nodes/
│   │           │       │   ├── CompletenessCheckNode.scala
│   │           │       │   ├── ConformityCheckNode.scala
│   │           │       │   ├── ConsolidatedDQReportNode.scala
│   │           │       │   ├── UniquenessCheckNode.scala
│   │           │       │   └── ValidityCheckNode.scala
│   │           │       └── utils/
│   │           │           └── ConfigLoader.scala
│   │           └── var/
│   │               └── pipeline/
│   │                   └── VarSvarNode.scala    # VaR/SVaR calculation engine
│   └── test/
│       └── scala/
│           └── com/
│               ├── dq/
│               │   └── pipeline/
│               │       ├── Helpers/
│               │       │   └── DQArithmeticAgentSpec.scala
│               │       ├── nodes/
│               │       │   ├── CompletenessCheckNodeSpec.scala
│               │       │   ├── ConformityCheckNodeSpec.scala
│               │       │   ├── ConsolidatedDQReportNodeSpec.scala
│               │       │   ├── UniquenessCheckNodeSpec.scala
│               │       │   └── ValidityCheckNodeSpec.scala
│               │       └── testutil/
│               │           └── SparkTestSession.scala
│               └── var/
│                   └── pipeline/
│                       └── nodes/
│                           └── VarSvarNodeSpec.scala     # VaR/SVaR test suite
└── README_MAVEN.md                   # Maven configuration guide
```

## Key Features

### Data Quality Pipeline

The DQ pipeline performs comprehensive data quality checks:

- **Completeness Check**: Identifies missing values and null records
- **Conformity Check**: Validates data types and formats
- **Validity Check**: Ensures values fall within expected ranges
- **Uniqueness Check**: Detects duplicate records and duplicate key combinations
- **Consolidated Report**: Aggregates all DQ metrics into a single report

### Value at Risk Calculation

The VaR/SVaR module implements historical simulation methodology:

**VaR Calculation:**
- **Window**: 252-day rolling window (1 trading year)
- **Percentile**: 99th percentile of returns
- **Time Horizon**: 10-day scaling using √10 factor
- **Formula**: VaR₁₀ₐᵧ,₉₉ = √10 × Q₀.₉₉({-rₜ over 252 days})

**Stressed VaR Calculation:**
- Same methodology as VaR but restricted to crisis periods (2008, 2020)
- Requires minimum 20 stressed observations
- Captures tail risk during market stress

**Output Metrics:**
- `VaR1Day99Rolling`: Daily 99th percentile value-at-risk
- `VaR10Day99Rolling`: 10-day scaled value-at-risk
- `SVaR1Day99Rolling`: Daily stressed value-at-risk
- `SVaR10Day99Rolling`: 10-day scaled stressed value-at-risk

## Building the Project

### Prerequisites
- Java 17 or higher (Java 8+ required, but 17 recommended for module support)
- Maven 3.6+
- Scala 2.12.x
- PostgreSQL with sp500stocks database (optional, for data persistence)

### Build Commands

#### Clean and Compile
```bash
mvn clean compile
```

#### Run Tests
```bash
mvn test

# Run specific test class
mvn test -Dtest=VarSvarNodeSpec

# Build without running tests
mvn clean package -DskipTests
```

#### Package (Create JAR)
```bash
mvn package
```

#### Run with Maven
```bash
# Using maven-exec plugin
mvn exec:java -Dexec.mainClass="com.varproject.DQMain"

# Or run the packaged JAR directly
java -cp target/var-project-1.0-SNAPSHOT-jar-with-dependencies.jar com.varproject.DQMain
```

#### Build with Java 17 Specifically
```bash
JAVA_HOME=/usr/libexec/java_home -v 17 mvn clean package
```

## Running the Application

### Method 1: Using Spark Submit
```bash
# Run the DQ pipeline with Spark
./run.sh

# Run with specific modules
./run.sh completeness
./run.sh conformity
./run.sh validity
./run.sh uniqueness
./run.sh varsvar
```

### Method 2: Standalone Mode
```bash
# Run in standalone mode (useful for development/testing)
./run-standalone.sh varsvar
```

### Method 3: Direct Maven Execution
```bash
mvn exec:java -Dexec.mainClass="DQMain"
```

## Configuration

### Configuration Files

Database credentials and application settings are loaded from `src/main/resources/application.conf` and can be overridden with environment variables in a `.env` file (not checked into git).

### Configuration Parameters

Edit `src/main/resources/application.conf` to configure:
- Database connections (PostgreSQL sp500stocks)
- Data file paths
- Pipeline parameters
- Output destinations
- Spark settings

Example configuration:
```
application {
  name = "DQ-VAR-Pipeline"
  version = "1.0.0"
  
  spark {
    app_name = "dq-var-analysis"
    master = "local[*]"
  }
  
  data {
    input_path = "data/sp500_data.csv"
    output_path = "output/"
  }
  
  database {
    host = "localhost"
    port = 5432
    database = "sp500stocks"
    user = "postgres"
    password = "password"
  }
}
```

## Testing

The project includes comprehensive unit tests using ScalaTest:

```bash
# Run all tests
mvn test

# Run tests with Java 17 module exports
JAVA_HOME=/usr/libexec/java_home -v 17 \
JDK_JAVA_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED" \
mvn test

# Run with coverage
mvn test -Dmode=coverage
```

### Test Coverage

- **DQArithmeticAgentSpec**: Arithmetic operations validation
- **CompletenessCheckNodeSpec**: Missing value detection
- **ConformityCheckNodeSpec**: Data type validation
- **ValidityCheckNodeSpec**: Value range validation
- **UniquenessCheckNodeSpec**: Duplicate detection
- **ConsolidatedDQReportNodeSpec**: Report aggregation
- **VarSvarNodeSpec**: VaR/SVaR calculations
  - Portfolio returns calculation
  - VaR with 252-day rolling window
  - SVaR with crisis period filtering
  - DataFrame structure and joins
  - Edge cases (single ticker)

## Data Onboarding

### Python Data Loading

Load S&P 500 historical data:

```bash
cd data_onboarding
python3 onboard_data.py
```

This script:
- Fetches S&P 500 price data
- Validates data quality
- Stores in project database
- Prepares for analysis

## Exploratory Data Analysis

Open the Jupyter notebook for interactive analysis:

```bash
cd EDA
jupyter notebook eda_var_analysis.ipynb
```

Analysis includes:
- Price trends and distributions
- Returns analysis
- Risk metrics visualization
- Correlation analysis

## Java 17 Compatibility

This project uses Java 17 and requires module access flags for Apache Spark:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 17)
export JDK_JAVA_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED \
  --add-exports=java.base/sun.util.calendar=ALL-UNNAMED \
  --add-exports=java.base/sun.security.action=ALL-UNNAMED \
  --add-exports=java.base/sun.security.util=ALL-UNNAMED"

mvn clean package
mvn test
```

The `pom.xml` includes these flags in both `maven-surefire-plugin` and `scalatest-maven-plugin` configurations.

## Output

### Data Quality Reports

Generated in `output/dq_reports/`:
- `completeness_report.csv`: Missing value metrics
- `conformity_report.csv`: Type validation results
- `validity_report.csv`: Range violation details
- `uniqueness_report.csv`: Duplicate detection
- `consolidated_dq_report.csv`: Complete DQ summary

### VaR Calculations

Generated in `output/var_results/`:
- `var_results.csv`: VaR and SVaR metrics by ticker and date
- Columns: Ticker, Date, VaR1Day99Rolling, VaR10Day99Rolling, SVaR1Day99Rolling, SVaR10Day99Rolling

## Troubleshooting

### Java Module Access Errors
If you encounter `IllegalAccessError` related to `sun.nio.ch.DirectBuffer`:
```bash
export JDK_JAVA_OPTIONS="--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
```

### Memory Issues
Increase Spark memory allocation:
```bash
export SPARK_DRIVER_MEMORY=4g
export SPARK_EXECUTOR_MEMORY=4g
```

### Python Dependency Issues
Install required packages:
```bash
pip install pandas numpy yfinance
```

## Performance Considerations

- **VaR Calculation**: 252-day rolling window; set to lower values for testing
- **Window Operations**: No partition defined; optimizations available for large datasets
- **Spark Parallelism**: Adjust `spark.default.parallelism` in configuration for cluster size
- **Memory**: Allocate sufficient memory for large ticker universes

## Contributing

When adding new features:
1. Create corresponding unit tests (following existing test patterns)
2. Update configuration if new parameters needed
3. Document in this README
4. Run full test suite: `mvn test`

## License

[Specify your license here]

## Contact

For questions or issues, please contact the development team.

## Version History

- **1.0.0** (2025-12-17): Initial release with DQ pipeline and VaR/SVaR calculations
