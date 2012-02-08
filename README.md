A simple work queue.

Description
-----------

This is a simple work queue written in python.  

The work queue does not require root privilege to run.  It does not require
daemons running on any of the work nodes in the cluster.  A server instance can
be run by any user and other users schedule jobs using a client.  When
scheduled to run, the client logs into the appropriate node using ssh and then
executes the job.

Users should have ssh keys and an ssh agent running to allow ssh login to the
nodes without typing their pass-phrase.

The only queue currently supported is a very simple matching queue with
priorities and limits.  This is **very** simple: jobs are put in the queue in
order they arrive.  Each time the queue is refreshed, the first one that can
run will run, with higher priority jobs checked first.  Users can set
requirements that must be met for jobs to run, e.g. machines must have a
certain amount of memory or number of cores, or be from a specific group of
machines. There is a special priority "block" that blocks other jobs until it
can run.  Users can also set limits on the number of jobs they run and/or the
number of cores they use.  These limits help relieve congestion.

Another queue could be plugged in if desired.

The wq Script
-------------

All operations are performed using the wq script (short for "work queue"), such
as running the server, starting jobs, listing the queue, etc.  You specify a
command and a set of arguments and options for that command. e.g.  to submit
jobs

    wq sub [options] [args]

To get help for wq use "wq -h".  To get help on a wq command, use "wq command
-h".

Submitting Jobs
---------------

You can either submit using a job file, written in YAML, or by sending the
commands as an option

    wq sub -b job_file1 job_file2 ...
    wq sub -c command
    wq sub job_file 

A job file contains a "command" and a set of requirements; see the Job Files
section for more details.  

If -b/--batch is sent, the job or jobs are submitted in batch mode in the
**background**, whereas normaly jobs are kept in the foreground.  Batch mode
also allows submission of multiple jobs. When submitting multiple jobs, a short
delay is observed between submissions to prevent overloading the server. 

When not using batch mode, you can also send requirements using -r/--require
    
    wq sub -r requirements job_file
    wq sub -r requirements -c command

Requirements sent using -r will over-ride those in the job file.  For a list of
available requirements fields, see the Requirements sub-section.

Note if you need to keep the outputs of your command, you need to redirect them
to files yourself.

###  Job Files

The job files and requirements are all in YAML syntax
<http://en.wikipedia.org/wiki/YAML>.  For example, to run the command "dostuff"
on a single core, and redirect the stdout and stderr to files, this would be
the job file

    command: dostuff 1> dostuff.out 2> dostuff.err

Don't forget the space between the colon ":" and the value.  The command can
actually be a full script.  Just put a **pipe symbol "|"** after command: and
then **indent the lines**.  For example

    command: |
        source ~/.bashrc
        cd ~/mydata
        cat data.txt | awk '{print $3}' 1> list.txt 2> list.err

You can also put requirements in the job file.  For example, to grab 3 cores
and only use  nodes from groups gen1 and gen2, but not group slow

    command: dostuff 1> dostuff.out 2> dostuff.err
    N: 3
    group: [gen1, gen3]
    notgroup: slow

Note group/notgroup are special in that they can take either a scalar or a
list. You can also specify lists using note-taking notation

    group:
        - gen1
        - gen2

Don't forget the space between dash "-" and value. See the Requirements
sub-section for a full list of requirements


### specifying comands as arguments

In addition to using job files, you can run a command by specifying -c and an
argument

    wq sub -c script

Remember to quote commands that have spaces/arguments.   For example, 

    wq sub -c "cd /some/dir; script -a input"

### Sending Requirements on the Command Line

You can specify requirements on the command line using -r/--require.

    wq sub -r "mode: bynode; N: 5" -c some_command

Each requirement is valid YAML. Note, however, that each element is separated
by a semicolon, which is **not** valid YAML.  Internally the semicolons are
replaced by newlines.  Also, you are allowed to leave off the required space
between colon ":" and value; again this is **not** valid YAML but these are put
in for you just to allow compact requirements strings.  After these
pre-processing steps, the requirements are parsed just like a job file.

If you need a semicolon in your requirements, or if adding a space after colons
causes problems, try using a full job file.

### Requirements

By default, a job is simply assigned a single core on the first available node.
You can use requirements to change what nodes are selected for your job. The following
is the full list

* mode - The mode of node selection.  Available modes are
 * bycore - Select single cores.  Modifiers like *N* refer to number of cores.
 * bycore1 - Select cores from a single node.
 * bynode - Select full nodes.  Modifiers like *N* refer to number of nodes.
 * byhost - Select a particular host by name.  Modifiers like *N* refer to number of cores.
 * bygroup - Select **all** the nodes from particular groups; different from the *group* requirement.
* N - The number of nodes or cores, depending on the mode.
* group - Select cores or nodes from the specified group or groups.  This can be a scalar or list
* notgroup - Select cores or nodes from machines not in the specified group or groups.
* host - The host name. When mode is byhost, you must also send this requirement
* min_cores - Limit to nodes with at least this many cores.  Currently only applies when mode is *bynode* (should this work for bycore selections?).
* min_mem - Limit to nodes with at least this much memory in GB.  Currently only applies when mode is *bycore*, *bycore1*, *bynode*.
* X - This determines if ssh X display forwarding is used, default is False. For yes use true,1 for no use false,0
* priority - Currently should be one of 
 * low - lowest priority
 * med - medium priority, the default
 * high - high priority
 * block - block other jobs until this one can run.
* job_name - A name to display in job listings. Usually the command, or an abbreviated form of the command, is shown.
* hostfile - An optional file in which to save allocated node names. Useful for MPI jobs using mpirun.


Here is a full, commented example

    # these are the commands to be run.  if you only have a 
    # single command, you can use a single line such as 
    # command: ./script

    command: |
        source ~/.bashrc
        mpirun -hostfile hfile ./program

    hostfile: hfile

    # show this name in job listings instead of the command
    job_name: dostuff35 

    # this is the type of node/host selection. bynode means select entire
    # nodes.
    mode: bynode

    # Since the mode is bynode, this means 5 full nodes
    N: 5

    # Select from this group(s)
    group: new

    # Do not select from this set of groups
    notgroup: [slow,crappy]

    # require at least this many cores
    min_cores: 8

### running an MPI code


To run an MPI code, get hostnames through the hostfile requirement, e.g.

    wq -r "N:16;hostfile:hfile" -c "mpirun -hostfile hfile ./mycode"



Getting an interactive shell on a worker node
---------------------------------------------

For an interactive shell, just use your login shell as the command, e.g.
"bash" or "tcsh".  If you need the display for graphics, plotting, etc. make
sure to send the X requirement.  e.g.

    wq sub -c bash
    wq sub -r "X:1" -c bash

Note you can use tcsh if that is your login shell.  In this scenario, your
environment will be set up as normal.

Placing limits on how many jobs you run or cores you use
--------------------------------------------------------

You can limit the number of jobs you can run at once, or the number
of cores you use.  For example, to limit it 25 jobs

    wq limit "Njobs: 25"

You can also specify Ncores, or even combine them

    wq limit "Njobs: 25; Ncores: 100"

These data are saved in a file on disk and reloaded when the server is
restarted.  You can remove limits by setting them to -1, e.g.

    wq limit "Njobs: -1; Ncores: -1"

Tips and Tricks
---------------

* Normally your environment is not set up when you run a command unless the
  command runs a login shell like "bash" or "screen".  You can get your setup
  by sourcing your startup script. e.g. 

        wq sub -c "source ~/.bashrc; command"
  You can also just run a script that sets up your environment and runs the
  command.

Getting Statistics For the Cluster and Queue
--------------------------------------------

### Job Listings 

To get a job listing us "ls".  Send -f or --full to see a full job list as a
YAML file.  This includes the full nodes list and the full command line.  You
can read the YAML from this stream and process it as you wish. Send -u/--user
to restrict the job list to a particular user or list of users (comma
separated).

    wq ls
    wq ls -f
    wq ls -u username
    wq ls -u user1,user2 -f

Here is an example of a normal listing

    Pid   User St Pri Nc Nh Host0            Tq      Trun Cmd     
    29939 anze R  med 2  1  astro0029 15h09m58s 15h09m58s run23
    29944 anze W  low -  -  -         15h09m42s         - mock_4_5
    29950 anze R  med 2  1  astro0010 15h09m18s 12h42m55s run75
    Jobs: 3 Running: 2 Waiting: 1

Pid is the process id, St is the status (W for waiting, R for running), Pri is
the priority, Nc is the number of cores, Nh is the number of hosts/nodes, Host0
is the first host in the hosts list, Tq is the time the job has been in the
queue, Trun is the time it has been running, and Cmd is the job_name, if given
in the requirements, otherwise it is the first word in the command line.

The default job listing will always have a fixed number of columns except for
the summary line. White space in job names will be replaced by dashes "-" to
guarantee this is always true.  This guarantees you can run the output through
programs like awk.

### Cluster and Queue Status

Use the "stat" command to get a summary of the cluster usage and queue
status.

    wq stat

For each node, the usage is displayed using an asterisk * for used cores and a
dot . for unused cores.  for example [***....] means three used and 4 unused
cores.  Also displayed is the memory available in gigabytes and the groups for
each host.

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


### User information

Using the users command, you can list the users of the system, the number of
jobs running, the number of cores used, and the user's limits:

    wq users

Here is an example listing

    User      Njobs  Ncores  Limits
    esheldon  10     80      {Ncores:100;Njobs:10}
    anze      35     35      {}

Refreshing the Queue
--------------------

The server refreshes every 30 seconds by default.  To request a refresh
use the "refresh" command

    wq refresh

Removing Jobs
-------------

To remove a job or jobs from the queue, send the "rm" command

    wq rm pid

Where pid is the process id you can get using "wq ls".  The pid can be a comma
separated list.  To remove all of your jobs

    wq rm all

Only root can remove jobs for another user.

Starting a Server
-----------------

    wq serve cluster_description

The cluster description file has a line for each work node in your cluster.
The format is

    hostname ncores mem groups


The mem is in gigabytes, and can be floating point.  The groups are optional
and comma separated.  

Unless you are just testing, you **almost certainly** want to run it with nohup
and redirect the output

    nohup serve desc 1> server.out 2> serve.err &

You can change the port for sockets using -p; 

    wq -p portnum serve descfile

the clients will also need to use that port.

    wq -p portnum sub jobfile

### The Spool Directory

The job and user data are kept in the spool directory, ~/wqspool by
default.  So if you restart the job from a different account, remember to
specify -s/--spool when starting the server.

    wq serve -s spool_dir desc

### Restarting the server

When you restart the server, all jobs and user data will be reloaded.  Note the
port will typically be "in use" from the previous instance for 30 seconds or
so, so be patient; it is no big deal for the server to be off for a while, it
will catch up.  Users will just have to wait a bit to submit jobs.

Installation
------------

### Dependencies

You need a fairly recent python and pyyaml <http://pyyaml.org/>

### Install

Get the source, untar it, cd into the created directory, and type this
to install into the "usual" place

    python setup.py install

To install into a particular prefix

    python setup.py install --prefix=/some/path


Authors
-------

Erin Sheldon: erin dot sheldon at gmail dot com

Anze Slosar: anze at bnl dot gov
