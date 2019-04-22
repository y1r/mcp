#include <mpi.h>

#include <getopt.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <cassert>
#include <iostream>
#include <vector>

// 512-bytes aligned
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

void copy_file_using_direct_io(const std::string &from, const std::string &to,
                               mode_t mode, size_t iterations, int mpi_rank,
                               int verbose) {
    int read_fd = -1;
    int write_fd = -1;

    void *buffer = aligned_alloc(512, BLOCKSIZE);

    if (mpi_rank == ROOT) {
        read_fd = open(from.c_str(), O_RDONLY | O_DIRECT);
        assert(read_fd != -1);
    }

    write_fd = open(to.c_str(), O_WRONLY | O_CREAT | O_DIRECT, mode);
    assert(write_fd != -1);

    double elapsed = 0;
    for (size_t i = 0; i < iterations; i++) {
        double start = get_time();
        {
            if (mpi_rank == ROOT) {
                read(read_fd, buffer, BLOCKSIZE);
            }
            MPI_Bcast(buffer, BLOCKSIZE, MPI_BYTE, ROOT, MPI_COMM_WORLD);

            write(write_fd, buffer, BLOCKSIZE);
        }
        double end = get_time();

        elapsed += end - start;

        if (verbose) {
            std::cout << "rank: " << mpi_rank << ", received: " << BLOCKSIZE
                      << ", on: " << i * BLOCKSIZE / elapsed / 1000 / 1000
                      << "[MB/s]" << std::endl;
        }
    }

    free(buffer);

    if (read_fd != -1) close(read_fd);
    if (write_fd != -1) close(write_fd);
}

void copy_file_with_offset(const std::string &from, const std::string &to,
                           mode_t mode, size_t offset, size_t bytes,
                           int mpi_rank) {
    if (bytes == 0) return;

    int read_fd = -1;
    int write_fd = -1;

    void *buffer = malloc(bytes);

    if (mpi_rank == ROOT) {
        read_fd = open(from.c_str(), O_RDONLY);
        assert(read_fd != -1);
    }

    write_fd = open(to.c_str(), O_WRONLY);
    assert(write_fd != -1);

    if (mpi_rank == ROOT) {
        pread(read_fd, buffer, bytes, offset);
    }

    MPI_Bcast(buffer, bytes, MPI_BYTE, ROOT, MPI_COMM_WORLD);

    pwrite(write_fd, buffer, bytes, offset);

    free(buffer);

    if (read_fd != -1) close(read_fd);
    if (write_fd != -1) close(write_fd);
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

    size_t direct_io_iterations = filesize / BLOCKSIZE;

    copy_file_using_direct_io(from, to, mode, direct_io_iterations, mpi_rank,
                              verbose);

    size_t transferred = direct_io_iterations * BLOCKSIZE;

    copy_file_with_offset(from, to, mode, transferred, filesize - transferred,
                          mpi_rank);

    MPI_Finalize();

    return 0;
}
