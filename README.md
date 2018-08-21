# ElaSQL

[![Build Status](https://travis-ci.org/elasql/elasql.svg?branch=master)](https://travis-ci.org/elasql/elasql)
[![Apache 2.0 License](https://img.shields.io/badge/license-apache%202.0-orange.svg)](https://www.apache.org/licenses/LICENSE-2.0)
[![Maven Central](https://img.shields.io/maven-central/v/org.elasql/elasql.svg)](https://maven-badges.herokuapp.com/maven-central/org.elasql/elasql)

ElaSQL is a distributed relational database system prototype that aims at offering high scalability, high availability, and elasticity to the on-line transaction processing (OLTP) applications.

## Key Features

- Highly Available, Strong Consistent
- High Scalability
- Elastic and Economic (Work in Progress)

See [the offical website](http://www.elasql.org/) for more information.

## Required Tools

You will need the following tools to compile and run this project:

- Java Development Kit 1.7 (or newer)
- Maven

## Getting Started

We recommend to start from our benchmark framework, ElaSQL-Bench, to connect and test ElaSQL. ElaSQL-Bench contains various benchmarks that often used in both academic and industry.

See [ElaSQLBench](https://github.com/elasql/elasqlbench) for more information about benchmarking ElaSQL.

## System Configurations

Check out our [configuration instructions](doc/configurations.md).

## Linking via Maven

```xml
<dependency>
  <groupId>org.elasql</groupId>
  <artifactId>elasql</artifactId>
  <version>0.2.1</version>
</dependency>
```

## Contact Information

If you have any question, you can either open an issue here or contact [elasql@datalab.cs.nthu.edu.tw](elasql@datalab.cs.nthu.edu.tw) directly.

## License

Copyright 2016-2018 elasql.org contributors

Licensed under the [Apache License 2.0](LICENSE)