qtask
===

This is a set of utilities for submitting jobs to a job scheduler (PBS or SGE) or organizing jobs into monolithic bash scripts. The point of this is to produce a library capable of automating the process of submitting jobs as part of a data analysis pipeline. It is also useful for running the same commands using multiple input files in a parallel manner.

Examples:

	mqsub -walltime '2:00:00' filter.sh '{}' -- files*.fastq.gz
	mypipeline.py -name 'foo' infile ref.fa rrna.fa


You can configure which job scheduler you use by creating a file named $HOME/.qtaskrc. Here is an example file:

	runner = sge
	runner.parallelenv = shm
	runner.multiplier = 2.0
	monitor = sqlite://~/qtask-jobs.db

