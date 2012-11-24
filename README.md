# Pwrake

Parallel workflow extension for Rake
* Author: Masahiro Tanaka

TODO: Write a gem description

## Features

* Parallize all tasks; No need to modify Rakefile.
* Given number of worker threads.
* Remote exuecution using SSH.
* Locality-aware node-selection for Gfarm file system.

## Installation

Download source tgz/zip and expand, cd to subdir and install:

    $ ruby setup.rb

Or gem install it as:

    $ gem install pwrake

## Usage

Parallel execution using 4 cores at localhost:

    $ pwrake -j 4

Parallel execution using all cores at localhost:

    $ pwrake -j

Parallel execution using total 2*2 cores at remote hosts listed in a 'hosts' file:

    $ cat hosts
    host1 2
    host2 2
    $ pwrake --hostfile=hosts

## Workflow demo

## Contributing

1. Fork it
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create new Pull Request
