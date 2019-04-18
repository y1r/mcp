#include <mpi.h>

#include <sys/time.h>
#include <getopt.h>

#include <cassert>
#include <iostream>
#include <vector>

constexpr long long BLOCKSIZE = 128 * 1024 * 1024;

inline double get_time()
{
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec * 1e-6;
}

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

    MPI_Info info;
    MPI_Info_create(&info);

    MPI_Info_set(info, "mpiio_coll_contiguous", "true");
    MPI_Info_set(info, "coll_read_bufsize", "134217728");

    MPI_File fp;
    MPI_File_open(MPI_COMM_WORLD, filenames[0].c_str(), MPI_MODE_RDONLY,
                  info, &fp);

    MPI_File_set_atomicity(fp, 0);

    MPI_Offset filesize;
    MPI_File_get_size(fp, &filesize);

    std::cout << "rank: " << mpi_rank << ", filesize: " << filesize
              << std::endl;

    void *buffer = malloc(BLOCKSIZE);

    long long iterations = (filesize + (BLOCKSIZE - 1)) / BLOCKSIZE;

    for (int i = 0; i < iterations; i++) {
        long long previous_step = std::max(0LL, i * BLOCKSIZE);
        long long this_step = std::min(filesize, (i + 1) * BLOCKSIZE);

        int count = this_step - previous_step;

        double start = get_time();
        MPI_File_read_all(fp, buffer, BLOCKSIZE, MPI_BYTE, MPI_STATUS_IGNORE);
        double end = get_time();

        double elapsed = end - start;

        std::cout << "rank: " << mpi_rank << ", received: " << count
                  << "on: " << count / elapsed / 1000 / 1000 << "[MB/s]" << std::endl;
    }

    free(buffer);

    MPI_File_close(&fp);

    MPI_Finalize();

    return 0;
}
