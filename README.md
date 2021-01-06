This repository contains the code for Mitos, a data analytics system that compiles programs with imperatively written control flow in Emma to a single Flink dataflow job.

The Flink directory contains a fork of Flink, where we added the Control Flow Manager. The Emma directory contains a fork of Emma where we have added the compilation in the emma-mitos directory.

**Use build.sh to build.**

The other directories contain code for the baselines of the experiments in the paper.
