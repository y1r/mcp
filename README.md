# MCP: MPI Copy Utility
## MCP: cp command implementation by MPI

- mcp_mpiio.cpp
Use `MPI-IO` to aggregate a read operation.

- mcp_mpi_bcast.cpp
Use `MPI_Bcast` to broadcast.

- mcp_mpi_bcast_direct.cpp
Use `MPI_Bcast` to broadcast with `O_DIRECT` writing.

- mcp_mpi_bcast_direct_pipeline.cpp
Use `MPI_Bcast` to broadcast with `O_DIRECT` pipelined writing.

```
mpicxx mcp_mpi_bcast_direct_pipeline.cpp -o mcp
mpiexec -N 1 ./mcp ${from} ${dst}
```

## MCAT: cat command implementation by MPI
- mcat.cpp

```
mpicxx mcat.cpp -o mcat
mpiexec -N 1 copy-tar-and-extract.sh
```

(copy-tar-and-extract.sh)
```
#!/bin/bash

./mcp ${from}.tar | tar xf - -C ${dst}
```
