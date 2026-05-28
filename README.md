# JDS_development_template
Template repository for generating (development/exploration stage) repositories for use with jupyterdask tool (see JupyterDaskOnSlurm)

## Use of the template repository
This repository provides a simple template as basis for developing analysis that is to be deployed/run on an HPC system using the jupyterdask tool from the RS-DAT framework.

Starting from this repository users can create and manage their code. GitHub actions to provide containerized environments for deployment as well as a modifiable submission script for use with the jupyterdask cli tool are provided.

## Running a single Prefect flow

### Interactive use

```{bash}
srun -n 1 -c 4 -t 1:00:00 --pty /bin/bash
apptainer pull dask_prefect.sif oras://ghcr.io/hpc-dat/prefect-dask-poc:latest
export APPTAINER_IMAGE=$(realpath dask_prefect.sif)
# Bind all paths from host system within the container
_DIRS=`/usr/bin/ls -1 / | /usr/bin/awk '!/dev/' | /usr/bin/sed 's/^/\//g' `
export APPTAINER_BIND=`echo ${_DIRS} | /usr/bin/sed 's/ /,/g' `
# Turn off Prefect tracking
export DO_NOT_TRACK=1
apptainer run dask_prefect.sif python /src/flow.py
```

### Slurm batch job


## Disclaimer

PoC developed with Claude Code.
