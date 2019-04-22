#include <mpi.h>

#include <getopt.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <cassert>
#include <iostream>
#include <vector>

constexpr size_t BLOCKSIZE = 128 * 1024 * 1024;
constexpr int ROOT = 0;

inline double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec * 1e-6;
}

void arg_parse(int argc, char *argv[], int &verbose, std::string &from,
               std::string &to) {
    int c;

    int option_index = 0;

    struct option long_options[] = {
        {"verbose", no_argument, &verbose, 1},
        {0, 0, 0, 0},
    };

    while ((c = getopt_long(argc, argv, "", long_options, &option_index)) !=
           -1) {
    }

    // TODO(y1r): Support multiple files
    assert(optind == (argc - 2));

    from = std::string(argv[optind++]);
    to = std::string(argv[optind++]);
}

void init_mpi(int *argc, char **argv[], int &mpi_rank, int &mpi_size) {
    MPI_Init(argc, argv);

    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_rank);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_size);
}

size_t get_filesize(const std::string &filename) {
    struct stat stat_buf;

    assert(stat(filename.c_str(), &stat_buf) == 0);

    return stat_buf.st_size;
}

mode_t get_permission(const std::string &filename) {
    struct stat stat_buf;

    assert(stat(filename.c_str(), &stat_buf) == 0);

    return stat_buf.st_mode;
}

int main(int argc, char *argv[]) {
    std::string from, to;

    int verbose;
    arg_parse(argc, argv, verbose, from, to);

    int mpi_rank, mpi_size;
    init_mpi(&argc, &argv, mpi_rank, mpi_size);

    size_t filesize = 0;
    if (mpi_rank == ROOT) filesize = get_filesize(from);
    MPI_Bcast(&filesize, sizeof(filesize), MPI_BYTE, ROOT, MPI_COMM_WORLD);

    mode_t mode = 0;
    if (mpi_rank == ROOT) mode = get_permission(from);
    MPI_Bcast(&mode, sizeof(mode), MPI_BYTE, ROOT, MPI_COMM_WORLD);

    void *buffer1 = malloc(BLOCKSIZE);
    void *buffer2 = malloc(BLOCKSIZE);

    size_t iterations = (filesize + (BLOCKSIZE - 1)) / BLOCKSIZE;

    int read_fd = -1;
    int write_fd = -1;

    if (mpi_rank == ROOT) {
        // read_fd = open(from.c_str(), O_RDONLY | O_NONBLOCK);
        read_fd = open(from.c_str(), O_RDONLY);
        assert(read_fd != -1);
    }

    // write_fd = open(to.c_str(), O_WRONLY | O_CREAT | O_NONBLOCK);
    write_fd = open(to.c_str(), O_WRONLY | O_CREAT, mode);
    assert(write_fd != -1);

    for (size_t i = 0; i < iterations; i++) {
        size_t previous_step = std::max(static_cast<size_t>(0), i * BLOCKSIZE);
        size_t this_step = std::min(filesize, (i + 1) * BLOCKSIZE);

        size_t count = this_step - previous_step;

        double start = get_time();
        {
            if (mpi_rank == ROOT) {
                read(read_fd, buffer1, count);
            }
            MPI_Bcast(buffer1, count, MPI_BYTE, ROOT, MPI_COMM_WORLD);

            write(write_fd, buffer1, count);
        }
        double end = get_time();

        double elapsed = end - start;

        if (verbose) {
            std::cout << "rank: " << mpi_rank << ", received: " << count
                      << ", on: " << count / elapsed / 1000 / 1000 << "[MB/s]"
                      << std::endl;
        }
    }

    free(buffer2);
    free(buffer1);

    if (read_fd != -1) close(read_fd);
    if (write_fd != -1) close(write_fd);

    MPI_Finalize();

    return 0;
}
