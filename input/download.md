title: Download Arabesque
project: Arabesque
---

# Quick start

If you want to start using Arabesque on a single machine without first having to install and configure the Hadoop stack, you can choose one of the following options:

* [Virtualbox image](https://qbox.qcri.org/index.php/s/175XGYLVf2X5GzI)
* Docker image `docker run -it dsqcri/arabesque` or download [Arabesque Dockerfile](https://qbox.qcri.org/index.php/s/38wvzHdK2LYRGAs) and build the image on your local machine
* [Installation scripts](https://qbox.qcri.org/index.php/s/CC5IEhQc0RAvLYV) to install Arabesque stack on a single node

# Github repo

The source code can be downloaded from Github
* [Giraph](https://github.com/qcri/Arabesque/tree/master)
* [Spark](https://github.com/qcri/Arabesque/tree/spark-2.0)

Arabesque is licensed under Apache 2.0.

# Precompiled Java package

The precompiled jar contains the complete Arabesque system along with all its dependencies. It also includes 4 example applications:
clique finding, motif counting, frequent subgraph mining and triangle counting.

This jar is ideal if you simply want to test the example applications or manually setup your own project.

* [Download the latest Giraph JAR](https://qbox.qcri.org/s/ZpA823NgPQqDzB8)
* [Download the latest Spark JAR](https://qbox.qcri.org/s/xvlvdDPGiEFYNjK)

To run any of the example applications, refer to [our user guide](user_guide.html#how-to-run-an-arabesque-job).

# Preconfigured Maven project

If you want to start developing your own graph mining implementations on top of Arabesque, the easiest way to achieve this is by forking our [Arabesque-Skeleton](https://github.com/qcri/Arabesque-Skeleton) project and follow the instructions on the README file.

# Reproducing the results of the SOSP'15 paper

After the SOSP publication, we have substantially refactored the codebase in order to open source it and make it more easily readable. The result is the version of Arabesque available in the website. While cleaner in terms of code, this version leaves some performance on the table compared to the SOSP prototype. If you want to make a performance comparison and reproduce the performance numbers of the SOSP paper, you can download the original prototype [here](https://www.dropbox.com/s/7utts0o2atotq6t/Qanat.zip?dl=0).

The prototype is called Qanat. The archive is organized as following:

* src/ contains the source code for Qanat
* scripts/ contains different scripts to run Qanat
* jar/ contains the Qanat jar file
* README.md documents how to configure the parameters in order to run Qanat
