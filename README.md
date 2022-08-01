# Overview

This repository contains the updated implementation of the Chorus
system (version 0.1.3) for differential privacy, to accompany the
following paper:

- [**CHORUS: a Programming Framework for Building Scalable Differential
Privacy Mechanisms.**](https://conferences.computer.org/eurosp/pdfs/EuroSP2020-2psedXWK6U4prXdo7t91Gm/508700a535/508700a535.pdf) Noah Johnson, Joseph P. Near, Joseph
M. Hellerstein, Dawn Song. *EuroS&P 2020*.

This is an updated release of Chorus (version 0.1.3). The original
release of Chorus is available
[here](https://github.com/uber-archive/sql-differential-privacy); see
the original repository and the paper for more documentation.

## Building & Running

This framework is written in Scala and built using Maven. The code has been tested on Mac OS X and Linux. To build the code:

```
$ mvn package
```

## Running Examples

The file `examples/MechanismExamples.scala` contains several examples
from the paper. To run the examples, after building Chorus:

```
mvn exec:java -Dexec.mainClass="examples.MechanismExamples"
```

## Potential Security Issues

This repo contains a proof-of-concept implementation resulting from a
research project; we did not attempt to mitigate issues resulting from
floating-point arithmetic or side channels. In particular, the
following known attacks might be possible, depending on the deployment
scenario and DBMS you use:

- [On significance of the least significant bits for differential
  privacy](https://dl.acm.org/doi/pdf/10.1145/2382196.2382264). Ilya
  Mironov, CCS 2012.
- [Widespread Underestimation of Sensitivity in Differentially Private
  Libraries and How to Fix It](https://arxiv.org/abs/2207.10635).
  Casacuberta et al., preprint, July 2022.
- [Side-Channel Attacks on Query-Based Data
  Anonymization](https://dl.acm.org/doi/pdf/10.1145/3460120.3484751).
  Boenisch et al., CCS 2021.

Please don't use this code in production without considering these
issues first.

## License

This project is released under the MIT License.

## Contact Information

This code is maintained by [Joe Near](http://www.uvm.edu/~jnear/).
