QTask - simple library for writing processing pipelines in Python
===

## Overview

QTask is a library that makes it easier to write large data processing pipelines, such as those used in bioinformatics. QTask works best for workflows that can be automated and run on a cluster. Each task should consist of a small, independent unit of work, that takes some number of input files and writes some number of output files. This should not be confused with something like Hadoop where all of the processing takes place within the system. For QTask (and SGE-style clusters), the actual jobs submitted are small bash scripts. These bash scripts are auto generated by QTask and submitted to the scheduler for processing.

QTask works mainly through a combination of the `@qtask.task` decorator and a [Future object](http://en.wikipedia.org/wiki/Futures_and_promises). In traditional concurrent programming, a future object is some object whose value will be available at some point in time in the future. It is left up to the backend to perform whatever processing is needed to get the value. Normally, this all takes place in the same process. However, most bioinformatics pipelines require wiring together many different programs where the inputs and outputs are all files. Additionally, for most large processing clusters, jobs are managed by an dedeicated job scheduler (SGE/PBS/etc...). Taking this into account, for QTask, we are using the concept of a `FutureFile`. The `FutureFile` is a simple object with two properties: the output filename and the task definition that will be run to get the output file. Through the combination of the decorator and the `FutureFile`, a full dependency tree can be determined, allowing for submitting jobs with proper dependencies to a dedicated job scheduler (such as SGE or PBS).

The `@qtask.task` decorator is used to wrap a Python function that determines what job needs to run. This function returns a `dict` that contains *at minimum* the name of any output files, and the fragment of a bash script needs to run to obtain those files. The `dict` can also contain any other configuration values to configure the job for the scheduler (such as number of CPUs/slices, memory usage, or walltime). A full list of task options is listed in the documentation for the `QTask` object. The decorator itself can also be used to configure the required resources for a job. The supported options are ultimately subject to which job scheduler is being used. The returned `dict` will be transformed into a corresponding `FutureFile`, which can be passed as the input to any number of other `@qtask.task` decorated functions.

## Quick example

Here is a minimal example that will gzip compress a file:

	@qtask.task()
	def gzip(filename):
		return {
			'cmd': 'gzip %s' % filename,
			'output': '%s.gz' % filename
		}

This ultimately yields a `FutureFile` that can be used any place where the output filename would otherwise be used. When that FutureFile is given as an argument to another `@qtask.task` method, that job will be dynamically added to the new job as a dependency. With this simple construct, complex workflows and pipelines are easy to assemble.

For example, here is another task that will calculate the MD5 sum of a file.

	@qtask.task()
	def md5sum(filename):
		return {
			'cmd': 'md5 %s > %s.md5' % (filename, filename),
			'output': '%s.md5' % filename
		}


And finally, here is how you can combine both functions together to setup a pipeline.

	def pipeline(input_filename):
		qtask.init()

		gzfile = gzip(input_filename)
		md5sum(gzfile)

		qtask.submit()

This simple pipeline performs two tasks: gzip a file and calculate an MD5 hash of the compressed file. If the input file is named `foo.txt`, there will be two new files created: `foo.txt.gz`, and `foo.txt.gz.md5`. When the `qtask.pipeline.submit()` method is called, two jobs will be submitted to the job scheduler: the gzip job and the md5sum job. The md5sum job will list the gzip job as a dependency, so the correct processing order will be respected.


## Job settings

Valid job resource/arguments:

    walltime    HH:MM:SS
    mem         Memory needed for the job (ex: 3G)
    hold        should this job be held until manually released (default: False)
    mail        [e,a,ea,n]
    queue       named queue to use (usually auto-selected by scheduler)
    qos         QOS or SGE project to use
    wd          working directory (default to current)
    stdout      stdout file
    stderr      stderr file
    procs       processors per node (SGE: pe shm)
    env         Use current environment (default: True)
    account     The account to set (usually for resource billing)


Any of these can be set as part of the `@qtask.task` decoration. For example, let's revisit the gzip job from above. If we'd like to tell the job scheduler to allow this job to run for no more than 2 hours, we could do it like this:

	@qtask.task(walltime='2:00:00')
	def gzip(filename):
		return {
			'cmd': 'gzip %s' % filename,
			'output': '%s.gz' % filename
		}


Or like this:

	@qtask.task()
	def gzip(filename):
		return {
			'cmd': 'gzip %s' % filename,
			'output': '%s.gz' % filename,
			'walltime': '2:00:00'
		}


## Extras
In addition to a Python library for creating your own pipeline, there is also a utility script included that makes it easy to run simple one-liner scripts for multiple input files. `mqsub` (multiple qsub) is a script that accepts any of the `QTask` job resource settings. It will run a simple one-line script for each of the given input files.

Examples:

	mqsub -walltime 2:00:00 filter.sh {} -- files*.fastq.gz

Each {} will be substituted with one of the filenames after the `--` and submitted to the job scheduler as a separate job.

You can configure which job scheduler you use by creating a file named $HOME/.qtaskrc. Here is an example file:

	runner = sge
	runner.parallelenv = shm
	runner.multiplier = 2.0
	monitor = sqlite://~/qtask-jobs.db

