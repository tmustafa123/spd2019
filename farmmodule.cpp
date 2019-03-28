#include "mpi.h"
#include <iostream>
#include <vector>
#include <unistd.h>

#define EMITTER_RANK 0
#define COLLECTOR_RANK 1
#define EMITTER_WORKER_TAG 50
#define COLLECTOR_WORKER_TAG 51
#define n_points 10

struct Point
{
    double x, y, z;
};
void EmitterUnit(int size, MPI_Datatype dt_point, Point data[n_points], int myRank);
void WorkerUnit(int size, MPI_Datatype dt_point, int myRank);
void CollectorUnit(int size, MPI_Datatype dt_point, int myRank);
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
        EmitterUnit(size, dt_point, data, rank);
    }
    else if (rank == COLLECTOR_RANK)
    {
        CollectorUnit(size, dt_point, rank);
    }
    else
    {
        WorkerUnit(size, dt_point, rank);
    }
    MPI_Finalize();
    return 0;
}

void EmitterUnit(int size, MPI_Datatype dt_point, Point data[n_points], int myRank)
{
    int worker_rank = 1;
    printf("In Emitter node %d\n", myRank);
    for (int streamItem = 1; streamItem < 10; streamItem++)
    {
        // Reset Rank
        worker_rank = 1;
        for (worker_rank; worker_rank < size; worker_rank++)
        {
            MPI_Send(data, n_points, dt_point, worker_rank, EMITTER_WORKER_TAG, MPI_COMM_WORLD);
        }
    }
    /* Sent data of size 0 */
    for (worker_rank = 1; worker_rank < size; worker_rank++)
    {
        MPI_Send(data, 0, dt_point, worker_rank, EMITTER_WORKER_TAG, MPI_COMM_WORLD);
    }
    printf("Emitter Terminates %d\n", myRank);
}
void WorkerUnit(int size, MPI_Datatype dt_point, int myRank)
{
    Point data[n_points];
    MPI_Status status;
    int number_amount;
    printf("In Worker node %d\n", myRank);
    while (true)
    {
        // Recieve a stream item
        MPI_Recv(data, n_points, dt_point, 0, EMITTER_WORKER_TAG, MPI_COMM_WORLD, &status);

        // Add a 3 second delay
        usleep(3000000);

        // Check the size
        MPI_Get_count(&status, MPI_INT, &number_amount);
        // terminate if data of size 0 is recieved and trigger the collector
        if (number_amount == 0)
        {
            printf("%d Rank terminates\n", myRank);
            MPI_Send(data, 0, dt_point, COLLECTOR_RANK, COLLECTOR_WORKER_TAG, MPI_COMM_WORLD);
            break;
        }
        // Else process the data
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
void CollectorUnit(int size, MPI_Datatype dt_point, int myRank)
{
    Point data[n_points];
    MPI_Status status;
    printf("In Collector node %d\n", myRank);
    std::vector<int> bufferHoldRank;
    int worker_rank = 1;
    int number_amount;
    bool flag = true;

    while (flag)
    {
        // Loop in round robin
        for (worker_rank = 1; worker_rank < size; worker_rank++)
        {
            MPI_Recv(data, n_points, dt_point, worker_rank, COLLECTOR_WORKER_TAG, MPI_COMM_WORLD, &status);
            MPI_Get_count(&status, MPI_INT, &number_amount);
            // If worker already terminated
            if (number_amount == 0)
            {
                bufferHoldRank.push_back(worker_rank);
            }
            // Collector terminates
            if (bufferHoldRank.size() == size)
            {
                printf("Collector terminates\n");
                // Terminate the outer loop
                flag = false;
                break;
            }
        }
    }
}