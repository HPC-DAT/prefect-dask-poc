# JDS_development_template
Template repository for generating (development/exploration stage) repositories for use with jupyterdask tool (see JupyterDaskOnSlurm)

## Use of the template repository
This repository provides a simple template as basis for developing analysis that is to be deployed/run on an HPC system using the jupyterdask tool from the RS-DAT framework.

Starting from this repository users can create and manage their code. GitHub actions to provide containerized environments for deployment as well as a modifiable submission script for use with the jupyterdask cli tool are provided.

## Running a single Prefect flow

### Interactive use

```{bash}
apptainer pull dask_prefect.sif oras://ghcr.io/hpc-dat/prefect-dask-poc:latest
export APPTAINER_BIND="\
$(which sbatch):/usr/local/bin/sbatch,\
$(which squeue):/usr/local/bin/squeue,\
$(which scancel):/usr/local/bin/scancel,\
/usr/lib64/slurm,\
/usr/lib64/libmunge.so.2,\
/etc/slurm,\
/etc/passwd,\
/etc/group,\
/var/run/munge,\
/home/$USER"
export DO_NOT_TRACK=1
apptainer run dask_prefect.sif python /src/flow.py
```

### Slurm batch job


## Disclaimer

PoC developed with Claude Code.
