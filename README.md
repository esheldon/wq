A simple work queue that actually works

Description
-----------

This is a simple work queue written in python.  

The work queue does not require root privilege to run.  It does not require
daemons running on any of the work nodes in the cluster.  A server instance is
run by any user and other users communicate with the server using a client.
When scheduled to run, the client logs into the appropriate node using ssh and
then executes the job.

For best results, users should have ssh keys and an ssh agent running to allow
ssh login to the nodes without typing their pass-phrase.  When submitting many
jobs, it is appropriate to use "nohup" and put the client in the background.

The only queue currently supported is first in first out (FIFO), with
priorities.  Another could be plugged in easily.


Installation
------------

### Dependencies

You need a fairly recent python and pyyaml <http://pyyaml.org/>

### Install

Get the source, untar it, cd into the created directory, and type this
to install into the "usual" place

    python setup.py install --prefix=/some/path

To install into a particular prefix

    python setup.py install --prefix=/some/path


The "wq" Script
---------------

All operations are performed using the wq script (short for "work queue"),
such as running the server, starting jobs, listing the queue, etc.



Submitting Jobs
---------------

You can either submit using a job file, written in YAML, or by sending the
commands as an argument

    wq sub job_file 
    wq sub -c "commands"

The job file contains a "command" and a set of requirements; see the Job Files
section for more details.  You can also send requirements using -r/--require

    
    wq sub -r "requirements" job_file
    wq sub -r "requirements" -c "commands"

Requirements sent using -r will over-ride those in the job file.  For
a list of available requirements fields, see the Requirements sub-section.

### Putting Jobs in the Background

You may want to submit a large number of jobs at once.  This is most convenient
if the jobs go into the background.  The best way to do this is using
nohub and redirecting the output to a file.

    nohub wq sub job_file > jobfile.wqlog 2> jobfile.wqerr &

You should redirect the output becuase otherwise it will just go to a file
called nohup.out in your current working directory. You can of course put
both stderr/stdout in the same file using &> jobfile.wqlog &

### Getting an interactive shell on a worker node.

For an interactive shell, just use "bash" or your favorite shell as the
command.  If you need the display for graphics, plotting, etc. make sure
to send the X requirement.  e.g.

    wq sub -c bash
    wq sub -r "X:1" -c bash

###  Job Files

The job files and requirements are all in YAML
syntax <http://en.wikipedia.org/wiki/YAML>.  For example, to run the command
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

See the Requirements sub-section for a full list of requirements

### Sending Requirements on the Command Line

You can specify requirements on the command line using -r/--require.

    wq sub -r "mode: bynode; N: 5" -c some_command

Each requirement is valid YAML. Note, however, that each element is separated
by a semicolon, which is **not** valid YAML.  Internally the semicolons are
replaced by newlines, after which the result is parsed just like a job file.

### Requirements

By default, a job is simply assigned a single core on the first available node.
You can use requirements to change what nodes are selected for your job. The following
is the full list

* mode - The mode of node selection.  Available modes are
 * bycore - Select single cores.  Modifiers like *N* refer to number of cores.
 * bycore1 - Select single cores in a single node.
 * bynode - Select full nodes.  Modifiers like *N* refer to number of nodes.
 * byhost - Select a particular host by name.  Modifiers like *N* refer to number of cores.
 * bygroup - Select **all** the nodes from particular groups; different from the *group* requirement.
* N - The number of nodes or cores, depending on the mode.
* group - Select cores or nodes from the specified group or groups.  This can be a scalar or list
* notgroup - Select cores or nodes from machines not in the specified group or groups.
* min_cores - Limit to nodes with at least this many cores.  Currently only applies when mode is *bynode* (should this work for bycore selections?).
* X - This determines of X forwarding is used, default is False. For yes use true,1 for no use false,0
* priority - Currently should be one of low, med, high.  Higher priority jobs will be queued first.

Here is a full, commented example

    # this is the command to be run.
    command: mycommand

    # this is the type of node/host selection. bynode means select entire nodes.
    mode: bynode

    # Since the mode is bynode, this means 5 full nodes
    N: 5

    # Select from this group(s)
    group: gen1

    # Do not select from this set of groups
    notgroup: [slow,crappy]

    # require at least this many cores
    min_cores: 8


Getting Statistics For the Cluster and Queue
--------------------------------------------

### Job Listings 

To get a job listing us "ls".  Send -f or --full to see a full list of
nodes for each job and the full command line. Send -u/--user to restrict
the job list to a particular user.


    wq ls
    wq ls -f
    wq ls -u username --full

Here is an example of a normal listing

    Pid   User Status Priority Ncores Nhosts Command t_in      t_run    
    2530  anze R      med      132    11     source  12h50m27s 12h50m27s
    3246  anze R      med      104    13     source  12h24m28s 12h24m28s
    18743 anze W      med      -      -      source  29m52s    -        
    Jobs: Running: 2 Waiting: 1

### Cluster and Queue Status

Use the "stat" command to get a summary of the cluster usage and queue
status.

    wq stat

For each node, the usage is displayed using an asterisk * for used cores and a
dot . for unused cores.  for example [***....] means three used and 4 unused
cores.  Also displayed is the memory available and the labels/groups for each
host.

Here is an example

    usage           host      mem groups    
    [************]  astro0001  32 gen4,gen45
    [************]  astro0004  32 gen4,gen45
    [********....]  astro0010  48 gen5,gen45
    [............]  astro0011  48 gen5,gen45
    [....]          astro0016   8 gen1,slow 
    [*...]          astro0017   8 gen2,slow 
    [....]          astro0018   8 gen2,slow 
    [....]          astro0020   8 gen2,slow 
    [********]      astro0031  32 gen3      
    [****....]      astro0032  32 gen3      
    [........]      astro0033  32 gen3      


Refreshing the Queue
--------------------

The server refreshes every 30 seconds by default.  To request a refresh
use the "refresh" command

    wq refresh

Removing Jobs
-------------

To remove a job or jobs from the queue, send the "rm" command

    wq rm pid

Where pid is the process id you can get using "wq ls".  To remove
all of your jobs

    wq rm all

Only root can remove other jobs for another user.  TODO: allow
sending a list of pids to remove.

Starting a Server
-----------------

    wq serve cluster_description

The cluster description file has a line for each work node in your cluster.
The format is

    hostname ncores mem labels


The mem is in gigabytes, and can be floating point.  The labels are optional
and comma separated.  You can change the port for sockets using -p; the clients
will also need to use that port.

