# Flegel
Flegel is an open-source machine learning system which implements our newly proposed flexible synchronous parallel model.

## 1. Introduction
Existing machine learning systems typically employ Bulk Synchronous Parallel (BSP) to accelerate iterative computations in distributed environments. However, BSP usually establishes the synchronization locations between two consecutive iterations by pre-assigning workloads, i.e., the number of sample data points in each training batch. Such a design is simple but poses great performance challenges because fast workers must wait for slow, straggling ones, especially in a heterogeneous cloud cluster. Asynchronous Parallel (ASP) alternatives can bypass this performance bottleneck, but at expense of potentially losing convergence guarantees. Flegel thereby implements our newly designed Flexible Synchronous Parallel (FSP) by establishing synchronization barriers in a time-based manner, to overcome shortcomings of BSP and ASP. 

The Flegel project started at UMASS Amherst (USA) in 2017. Now it is continuously developing at Ocean University of China (China). Flegel is a Java framework implemented on top of Apache Hama 0.2.0-incubating. Features of Flegel are listed as below.

* ___Flexible Synchronous Parallel:___ It provides strict convergence guarantee by consistently updating parameters like BSP, as well as significant runtime reduction by completely unleashing the power of all workers like ASP.    
* ___Optimized Synchronization Interval:___ A robustness adjustor can adaptively and dynamically tune the synchronization interval, by continuously collecting runtime statistics and performing a linear fit estimation.     
* ___Lightweight Workload Balance:___ By asynchronously migrating data between fast workers and stragglers, it can keep all data spread the cluster evenly utilized, so as to equally contribute to convergence.     
* ___Algorithms Library:___ Flegel exposes uniform APIs to end-users, for easily programming various machine learning algorithms. Some representatives have been implemented as built-in examples, like KMeans and CNN.

## 2. Quick Start
This section describes how to configurate, compile and then deploy Flegel on a cluster consisting of three physical machines running Red Hat Enterprise Linux 6.4 32/64 bit (one master and two slaves/workers, called `master`, `slave1`, and `slave2`). Before that, Apache Hadoop should be installed on the cluster, which is beyond the scope of this document. 

### 2.1 Requirements
* Apache hadoop-0.20.2 (distributed storage service)  
* Sun Java JDK 1.6.x or higher version   
* Optional libraries: aparapi-3.0.0.jar, aparapi-jni-1.4.3.jar, bcel-6.5.0.jar, and scala-library-2.13.6.jar (for running Java on GPUs)   
* Optional tool: apache ant-1.7.1 or higher version (for compiling your own Flegel)     
Without loss of generality, suppose that Flegel is installed in `~/Flegel` and Java is installed in `/usr/java/jdk1.6.0_23`.

### 2.2 Deploying Flegel   
#### downloading files on `master`  
`cd ~/`  
`git clone https://github.com/FSPML/FSPML.git`  
`chmod 777 -R Flegel/`

#### configuration on `master`  
First, edit `/etc/profile` by typing `sudo vi /etc/profile` and then add the following information:  
`export Flegel_HOME=~/Flegel`   
`export Flegel_CONF_DIR=$Flegel_HOME/conf`  
`export PATH=$PATH:$Flegel_HOME/bin`  
After that, type `source /etc/profile` in the command line to make changes take effect.  

Second, edit configuration files in `Flegel/conf` as follows:  
* __termite-env.sh:__ setting up the Java path.  
`export JAVA_HOME=/usr/java/jdk1.6.0_23`  
* __termite-site.xml:__ configurating the Flegel engine.  
The details are shown in [termite-site.xml](https://github.com/FSPML/FSPML/conf/termite-site.xml).   
* __workers:__ settting up workers of Flegel.  
`slave1`  
`slave2`  

#### deploying  
Copy configurated files on `master` to `slave1` and `slave2`.  
`scp -r ~/Flegel slave1:.`  
`scp -r ~/Flegel slave2:.`  

### 2.3 Starting Flegel  
Type the following commands on `master` to start Flegel.  
* __starting HDFS:__  
`start-dfs.sh`  
* __starting Flegel after NameNode has left safemode:__  
`start-termite.sh`  
* __stopping Flegel:__  
`stop-termite.sh`  

### 2.4 Running a KMeans job on `master`  
First, create an input file under input/sample on HDFS. Input file should be in format of:  
`sample_id \t dimension_1:dimension_2:...`  
An example is given in [random_sample](https://github.com/FSPML/FSPML/random_sample). You can download it and put it onto your HDFS:  
`hadoop dfs -mkdir input`  
`hadoop dfs -put random_sample input/`  
Currently, Flegel simply evenly partitions the input sample data across workers.   

Second, submit the KMeans job:   
`termite jar $Flegel/termite-examples-0.1.jar kmeans input output 16 1000 11000000 100 28 3 -1 0 3.5E7 -1`  
About arguments:  
[1] input directory on HDFS

[2] output directory on HDFS

[3] the number of child processes(int)

[4] the maximum number of iteration (int)

[5] the number of dataset

[6] the number of centroid (int, for K-means)

[7] the number of dimensions (int, for K-means)

[8] SyncModel (SemiAsyn => 3)

[9] idle_time per 1000 points (int, milliseconds, -1=>disabled, -2=>Multithreading simulation)

[10] the num ber of idle_task (int, 0=>disabled)

[11] converge_threshold (double)

[12] load migration ? (int, 1 => yes ,-1 => no, 2=>block)

## 3. Building Flegel with Apache Ant  
As an open-source system, users can also import source code into Eclipse as an existing Java project to modify the core engine, and then build your modified version. Before building, you should install Apache Ant 1.7.1 or higher version on your `master`. Suppose the modified version is located in `~/source/Flegel`.  You can build it using `~/source/Flegel/build.xml` as follows:  
`cd ~/source/Flegel`  
`ant`  
Notice that you can build a specified part of Flegel as follows:  
1) build the core engine  
`ant core.jar`  
2) build examples  
`ant examples.jar`   

By default, all parts will be built, and you can find `termite-core-0.1.jar` and `termite-examples-0.1.jar` in `~/source/Flegel/build` after a successful building. Finally, use the new `termite-core-0.1.jar` to replace the old one in `$Flegel_HOME` on the cluster (i.e., `master`, `slave1`, and `slave2`). At anytime, you should guarantee that the  `termite-core-xx.jar` file is unique in `$Flegel_HOME`. Otherwise, the starting script described in Section 2.3 may use a wrong file to start the engine.  

## 4. Programming Guide
Flegel includes some representative machine learning algorithms to show the usage of its APIs. These algorithms are contained in the `src/examples/` package and have been packaged into the `termite-examples-0.1.jar` file. Users can implement their own algorithms by learning these examples. After that, as described in Section 3 and Section 2.4, you can build your own algorithm can then run it.

## 5. Contact  
If you encounter any problem with Flegel, please feel free to contact wangzhigang@ouc.edu.cn.

