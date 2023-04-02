# Flux in Streamflow

From the root of the repository, build the container:

```bash
$ docker build -f examples/flux/Dockerfile -t streamflow-flux .
```

Shell into the container!

```bash
$ docker run -it streamflow-flux bash
$ whoami
```
```console
# fluxuser
```

Start a flux instance:

```bash
$ flux start --test-size=4 bash
```

Then go into the Flux example directory, and run the workflow.

```bash
$ cd examples/flux
$ streamflow run streamflow.yml 
```

That will compile a program and run it and exit. Note that the original example
uses `mpirun`, but since Flux has MPI bindings, we replace this with flux run.
You'll see the streamflow result printed to the screen:

```console
2023-04-02 19:35:18.426 INFO     COMPLETED Workflow execution
{
    "result": {
        "basename": "mpi_output.log",
        "checksum": "sha1$8abcdbccb5d53018e69ac1c1849f50928a6c4669",
        "class": "File",
        "dirname": "/code/examples/flux/ecc301a4-6fad-4199-b792-c47caaf7a9da",
        "location": "file:///code/examples/flux/ecc301a4-6fad-4199-b792-c47caaf7a9da/mpi_output.log",
        "nameext": ".log",
        "nameroot": "mpi_output",
        "path": "/code/examples/flux/ecc301a4-6fad-4199-b792-c47caaf7a9da/mpi_output.log",
        "size": 271
    }
}
2023-04-02 19:35:18.428 INFO     UNDEPLOYING dc-mpi
2023-04-02 19:35:18.443 INFO     COMPLETED Undeployment of dc-mpi
```

And the output directory will be in your working directory:

```bash
$ cat ecc301a4-6fad-4199-b792-c47caaf7a9da/mpi_output.log 
```
```console
Hello I'm the server with id 1 on bff3d5c1b83d out of 2 I'm the server
Hello I'm the server with id 0 on bff3d5c1b83d out of 2 I'm the server
Total time (MPI) 1 is 0.000105146
Total time (MPI) 0 is 0.000120071
Total time (gtd) 1 is 0.299063
Total time (gtd) 0 is 0.29899
```

## Development

To work in development mode, making changes on your local machine that
persist in the container (and you might want to change the user to ROOT)
in the Dockerfile:

```bash
$ docker run -it -v $PWD:/code streamflow-flux bash
```

Install!

```bash
$ pip install develop .
```

Note that for the example here, MPI doesn't like to be run as root, so you'll
get an error. Also note that because the queue managers are run async, it's
challenging to interactively develop.
