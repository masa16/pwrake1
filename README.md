# Pwrake

Parallel workflow extension for Rake
* Author: Masahiro Tanaka

([日本語README](https://github.com/masa16/pwrake/wiki/Pwrake.ja)),
([GitHub Repository](https://github.com/masa16/pwrake))

## Features

* Parallelize all tasks; no need to modify Rakefile, no need to use `multitask`.
* Tasks are executed in the given number of worker threads.
* Remote exuecution using SSH.
* Pwrake is an extension to Rake, not patch to Rake: Rake and Pwrake coexist.
* High parallel I/O performance using Gfarm file system.

## Installation

Download source tgz/zip and expand, cd to subdir and install:

    $ ruby setup.rb

Or, gem install:

    $ gem install pwrake

## Usage

### Parallel execution using 4 cores at localhost:

    $ pwrake -j 4

### Parallel execution using all cores at localhost:

    $ pwrake -j

### Parallel execution using total 2*2 cores at remote 2 hosts:

1. Share your directory among remote hosts via distributed file system such as NFS, Gfarm.
2. Allow passphrase-less access via SSH in either way:
   * Add passphrase-less key generated by `ssh-keygen`.  (Be careful)
   * Add passphrase using `ssh-add`.
3. Make `hosts` file in which remote host names and the number of cores are listed:

        $ cat hosts
        host1 2
        host2 2

4. Run `pwrake` with an option `--hostfile` or `-F`:

        $ pwrake --hostfile=hosts

## Options

### Command line option

        -F, --hostfile FILE              [Pw] Read hostnames from FILE
        -j, --jobs [N]                   [Pw] Number of threads at localhost (default: # of processors)
        -L, --logfile [FILE]             [Pw] Write log to FILE
            --ssh-opt, --ssh-option OPTION
                                         [Pw] Option passed to SSH
            --filesystem FILESYSTEM      [Pw] Specify FILESYSTEM (nfs|gfarm)
            --gfarm                      [Pw] FILESYSTEM=gfarm
        -A, --disable-affinity           [Pw] Turn OFF affinity (AFFINITY=off)
        -S, --disable-steal              [Pw] Turn OFF task steal
        -d, --debug                      [Pw] Output Debug messages
            --pwrake-conf [FILE]         [Pw] Pwrake configuation file in YAML
            --show-conf, --show-config   [Pw] Show Pwrake configuration options
        -h, -H, --help                   Display this help message.

### pwrake_conf.yaml

* If `pwrake_conf.yaml` exists at current directory, Pwrake reads options from it.
* Example (in YAML form):

        HOSTFILE : hosts
        LOGFILE : true
        TASKLOG : true
        PROFILE : true
        GNU_TIME : true
        PLOT_PARALLELISM : true
        DISABLE_AFFINITY: true
        DISABLE_STEAL: true
        FAILED_TARGET : delete
        PASS_ENV :
         - ENV1
         - ENV2

* Option list:

        HOSTFILE, HOSTS   default=false
        LOGFILE, LOG      default=none, string=filename, true="Pwrake%Y%m%d-%H%M%S_%$.log"
        TASKLOG           default=none, string=filename, true="Pwrake%Y%m%d-%H%M%S_%$.task"
        PROFILE           default=none, string=filename, true="Pwrake%Y%m%d-%H%M%S_%$.csv"
        WORK_DIR          default=$PWD
        FILESYSTEM        default=nil (autodetect)
        SSH_OPTION        (String) SSH option
        PASS_ENV          (Array) Environment variables passed to SSH
        GNU_TIME          If true, obtains PROFILEs using GNU time
        PLOT_PARALLELISM  If true, plot parallelism using GNUPLOT
        FAILED_TARGET     ( rename(default) | delete | leave ) failed files
        QUEUE_PRIORITY    ( DFS(default) | FIFO )

  for Gfarm system:

        DISABLE_AFFINITY  default=false
        DISABLE_STEAL     default=false
        GFARM_BASEDIR     default="/tmp"
        GFARM_PREFIX      default="pwrake_$USER"
        GFARM_SUBDIR      default='/'

## Note for Gfarm

* `gfwhere-pipe` command is required for file-affinity scheduling.

        wget https://gist.github.com/masa16/5787473/raw/6df5deeb80a4cea6b9d1d1ce01f390f65d650717/gfwhere-pipe.patch
        cd gfarm-2.5.8.1
        patch -p1 < ../gfwhere-pipe.patch
        ./configure --prefix=...
        make
        make install

## Tested Platform

* Ruby 2.0.0
* Rake 0.9.6
* CentOS 6.4

## Acknowledgment

This work is supported by
* JST CREST, research area: "Development of System Software Technologies for Post-Peta Scale High Performance Computing," and
* MEXT Promotion of Research for Next Generation IT Infrastructure "Resources Linkage for e-Science (RENKEI)."
