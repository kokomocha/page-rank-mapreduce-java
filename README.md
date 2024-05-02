<p align="center">  
    <br>
	<a href="#">
        <img height=100 src="https://cdn.svgporn.com/logos/java.svg" alt="Java" title="Java" hspace=20 />
        <img height=100 src="https://cdn.svgporn.com/logos/maven.svg" alt="Maven" title="Maven" hspace=20 />
        <img height=100 src="https://cdn.svgporn.com/logos/hadoop.svg" alt="Hadoop" title="Hadoop" hspace=20 /> 
  </a>	
</p>
<br>

# Page Rank Algorithm in Hadoop Map-Reduce 
(Coded for Homework-2 Assignment)

## Author
- Komal Pardeshi

## Installation
### Optional:
Install the WSL 2.0 from Microsoft site. [Click here](https://learn.microsoft.com/en-us/windows/wsl/install)
- Create Username and Password to gain root access as required for operations.

(All the components below this need to be installed on WSL.)
These components need to be installed first:
- OpenJDK 11
- Hadoop 3.3.5
- Maven (Tested with version 3.6.3)
- AWS CLI (Tested with version 1.22.34)

After downloading the hadoop installation, move it to an appropriate directory:
`mv hadoop-3.3.5 /usr/local/hadoop-3.3.5`

## Environment
1) Example ~/.bash_aliases:
	```
	export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
	export HADOOP_HOME=/usr/local/hadoop-3.3.5
	export YARN_CONF_DIR=$HADOOP_HOME/etc/hadoop
	export PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin
	```

2) Explicitly set `JAVA_HOME` in `$HADOOP_HOME/etc/hadoop/hadoop-env.sh`:
	`export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64`

## Execution
All of the build & execution commands are organized in the Makefile.
1) Unzip project file.
2) Open command prompt.
3) Navigate to directory where project files unzipped.
4) It is advsable to work in a virtual env.
5) For aws, update the Credentials in credentials file under `~/.aws/`.
6) Edit the Makefile to customize the environment at the top.
	- Sufficient for standalone: hadoop.root, jar.name, local.input
	Other defaults acceptable for running standalone.
	or 
	- For aws: aws.region, aws.bucket.name, aws.subnet.id,  cluse
7) Standalone Hadoop:
	- `make switch-standalone`		-- set standalone Hadoop environment (execute once)
	- `make local`
8)  **Pseudo-Distributed Hadoop Not Configured for this App** 
	
9) AWS EMR Hadoop: (you must configure the emr.* config parameters at top of Makefile)
	(Before Exectution)
	- `make make-bucket`			-- only before first execution
	- `make upload-input-aws`		-- only before first execution
	
	#### (aws.num.nodes, aws.instance.type)
	- `make aws`					-- check for successful execution with web interface for rs join app(aws.amazon.com)

	#### (Update aws.cluster.id)
	- `download-output-aws-rs`		-- downloads rs join output after successful execution & termination
	- `download-logs-aws-rs`		-- downloads rs join logs for execution

## Build Tested on
- Windows 10 with WSL-2.0
(All the components under Installation section need to be installed on WSL.)
