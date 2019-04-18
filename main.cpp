#include <mpi.h>

#include <getopt.h>

#include <cassert>
#include <iostream>
#include <vector>

constexpr int BLOCKSIZE = 16 * 1024 * 1024;

int main(int argc, char *argv[]) {
    int c;

    int verbose_flag = 0;
    int option_index = 0;

    struct option long_options[] = {
        {"verbose", no_argument, &verbose_flag, 1},
        {0, 0, 0, 0},
    };

    std::string from, to;

    while ((c = getopt_long(argc, argv, "", long_options, &option_index)) !=
           -1) {
    }

    std::vector<std::string> filenames;
    for (int i = optind; i < argc; i++) {
        filenames.emplace_back(argv[i]);
    }

    // TODO(y1r): Support multiple files
    assert(filenames.size() == 2);

    MPI_Init(&argc, &argv);

    int mpi_rank, mpi_size;
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);

    MPI_File fp;
    MPI_File_open(MPI_COMM_WORLD, filenames[0].c_str(), MPI_MODE_RDONLY,
                  MPI_INFO_NULL, &fp);

    MPI_Offset filesize;
    MPI_File_get_size(fp, &filesize);

    std::cout << "rank: " << mpi_rank << ", filesize: " << filesize
              << std::endl;

    char *buffer[BLOCKSIZE];

    int iterations = (filesize + (BLOCKSIZE - 1)) / BLOCKSIZE;
    MPI_Status status;

    for (int i = 0; i < iterations; i++) {
        MPI_File_read_all(fp, buffer, BLOCKSIZE, MPI_BYTE, &status);
        int count;
        MPI_Get_count(&status, MPI_BYTE, &count);
        std::cout << "rank: " << mpi_rank << ", received: " << count
                  << std::endl;
    }

    MPI_File_close(&fp);

    MPI_Finalize();

    return 0;
}
