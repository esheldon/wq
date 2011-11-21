A simple work queue that actually works

Description
-----------

This is a simple work queue written in python.  

The work queue does not require root privilege to run.  It does not require
daemons running on any of the work nodes in the cluster.  A server instance is
run by any user and other users communicate with the server using a client.
When scheduled to run, the client ssh logs into the appropriate node using ssh
and then executes the job.

For best results, users should have ssh keys and an ssh agent running to allow
ssh login to the nodes without typing their pass-phrase.  When submitting many
jobs, it is appropriate to use "nohup" and put the client in the background.

The only queue currently supported is first in first out (FIFO), with
priorities.  Another could be plugged in easily.

Dependencies
------------

You need a fairly recent python and a yaml parser; wq assumes works "import yaml".

Installation
------------

Get the source, untar it, cd into the created directory, and type this
to install into the "usual" place

    python setup.py install --prefix=/some/path

To install into a particular prefix

    python setup.py install --prefix=/some/path


The "wq" Script
---------------

All operations are performed using the wq script (short for "work queue"),
including running the server, starting jobs, listing the queue, etc.

Starting a Server
-----------------

    wq serve cluster_description

The cluster description file has a line for each work node in your cluster.
The format is

    hostname ncores mem labels


The mem is in gigabytes, and can be floating point.  The labels are optional
and comma separated.  You can change the port for sockets using -p; the clients
will also need to use that port.

Submitting Jobs
---------------

You can either submit using a job file, written in YAML, or by sending the
commands as an argument

    wq sub job_file 
    wq sub -c "commands"

The job file contains a "command" and a set of requirements; see the Job File
section for more details.  You can also send requirements using -r/--require

    
    wq sub -r "requirements" job_file
    wq sub -r "requirements" -c "commands"

Requirements sent using -r will over-ride those in the job file.  For
a list of available requirements fields, see the Requirements section.

Job Files
---------

The job files and requirements all to [YAML
syntax](http://en.wikipedia.org/wiki/YAML).  For example, to run the command
"script" on a single core, this would be the job file (without indentation)

    command: script

To do the same from the command line

    wq sub -c script

You can also put requirements in the job file.  For example, to grab 3 cores

    command: script
    N: 3

To only use nodes from a particular group, add a groups list

    group: [gen1, gen3]

or using note-taking notation

    group:
        - gen1
        - gen2

See the Requirements section for a full list of requirements

Requirements
------------

By default, a job is simply assigned a single core on the first available node.
You can use requirements to change what nodes are selected for your job. The following
is the full list

* mode - The mode of node selection.  Available modes are
** bycore
** bycore1
** bynode
** byhost
** bygroup
        
