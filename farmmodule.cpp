#include "mpi.h"
#include <iostream>

#define EMITTER_RANK 0
#define COLLECTOR_RANK 1
#define EMITTER_WORKER_TAG 50
#define COLLECTOR_WORKER_TAG 51
#define n_points 10

struct Point
{
    double x, y, z;
};
void EmitterNode(int size, MPI_Datatype dt_point, Point data[n_points], int myRank);
void WorkerNode(int size, MPI_Datatype dt_point, int myRank);
void CollectorNode(int size, MPI_Datatype dt_point, int myRank);
int main(int argc, char **argv)
{
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    MPI_Datatype dt_point;

    if (size < 2)
    {
        fprintf(stderr, "Farm size must be atleast 3 for %s\n", argv[0]);
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    MPI_Type_contiguous(3, MPI_DOUBLE, &dt_point);
    MPI_Type_commit(&dt_point);

    Point data[n_points];

    if (rank == EMITTER_RANK)
    {
        EmitterNode(size, dt_point, data, rank);
    }
    else if (rank == COLLECTOR_RANK)
    {
        CollectorNode(size, dt_point, rank);
    }
    else
    {
        WorkerNode(size, dt_point, rank);
    }
    MPI_Finalize();
    return 0;
}

void EmitterNode(int size, MPI_Datatype dt_point, Point data[n_points], int myRank)
{
    int flag = 0;
    printf("In Emitter node %d\n", myRank);
    for (int loop = 1; loop < 1000; loop++)
    {
        for (int worker_rank = 1; worker_rank < size; worker_rank++)
        {
            MPI_Send(data, n_points, dt_point, worker_rank, EMITTER_WORKER_TAG, MPI_COMM_WORLD);
        }
    }
    /* Sent a marker to all workers only once to terminate */
    for (int worker_rank = 1; worker_rank < size; worker_rank++)
    {
        MPI_Send(data, 0, dt_point, worker_rank, EMITTER_WORKER_TAG, MPI_COMM_WORLD);
    }
}
void WorkerNode(int size, MPI_Datatype dt_point, int myRank)
{
    Point data[n_points];
    MPI_Status status;
    int number_amount;
    printf("In Worker node %d\n", myRank);
    while (true)
    {
        MPI_Recv(data, n_points, dt_point, 0, EMITTER_WORKER_TAG, MPI_COMM_WORLD, &status);
        // After receiving the message, check the status to determine
        // how many numbers were actually received
        MPI_Get_count(&status, MPI_INT, &number_amount);
        if (number_amount == 0)
        {
            printf("%d Rank terminates\n", myRank);
            MPI_Send(data, 0, dt_point, COLLECTOR_RANK, COLLECTOR_WORKER_TAG, MPI_COMM_WORLD);
            break;
        }
        for (int i = 0; i < n_points; ++i)
        {
            data[i].x = (double)i;
            data[i].y = (double)-i;
            data[i].z = (double)i * i;
        }
        for (int i = 0; i < n_points; ++i)
        {
            std::cout << "Point #" << i << " : (" << data[i].x << "; " << data[i].y << "; " << data[i].z << ")"
                      << std::endl;
        }
        MPI_Send(data, n_points, dt_point, COLLECTOR_RANK, COLLECTOR_WORKER_TAG, MPI_COMM_WORLD);
    }
}
void CollectorNode(int size, MPI_Datatype dt_point, int myRank)
{
    Point data[n_points];
    MPI_Status status;
    printf("In Collector node %d\n", myRank);
    int worker_rank = 1;
    int number_amount;
    while (true)
    {
        for (worker_rank = 1; worker_rank < size; worker_rank++)
        {
            MPI_Recv(data, n_points, dt_point, worker_rank, COLLECTOR_WORKER_TAG, MPI_COMM_WORLD, &status);
            // After receiving the message, check the status to determine
            // how many numbers were actually received
            MPI_Get_count(&status, MPI_INT, &number_amount);
            if (number_amount == 0)
            {
                printf("%d Collector terminates\n", myRank);
                MPI_Send(data, 0, dt_point, COLLECTOR_RANK, COLLECTOR_WORKER_TAG, MPI_COMM_WORLD);
            }
            if (worker_rank == size)
                worker_rank = 1;
        }
    }
}