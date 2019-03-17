#include <mpi.h>
#include <stdio.h>
int main(int argc, char **argv)
{
    MPI_Init(&argc, &argv);
    int world_rank, world_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    double buf[10][12];
    MPI_Datatype column;
    MPI_Type_vector(10, 1, 12, MPI_DOUBLE, &column);
    MPI_Type_commit(&column);
    MPI_Send(buf[2], 1, column, 0, 0, MPI_COMM_WORLD);

    MPI_Finalize();
}