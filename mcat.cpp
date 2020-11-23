#include <mpi.h>

#include <getopt.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <unistd.h>

#include <cassert>

#include <iostream>

#include <condition_variable>
#include <mutex>
#include <thread>

#include <queue>
#include <vector>

// 512-bytes aligned
constexpr size_t BLOCKSIZE = 32 * 1024 * 1024;
constexpr int ROOT = 0;
constexpr size_t N_OF_BUFFERS = 4;

inline double get_time() {
    struct timeval tv;
    gettimeofday(&tv, NULL);

    return tv.tv_sec + tv.tv_usec * 1e-6;
}

template <typename T>
class ConcurrentQueue {
   public:
    T pop() {
        while (true) {
            std::unique_lock<std::mutex> lock(mtx_);

            // Check queue_ status and return when available.
            if (not queue_.empty()) {
                T v = queue_.front();
                queue_.pop();

                return v;
            }

            cond_.wait(lock);
        }
    }

    void push(T v) {
        {
            std::lock_guard<std::mutex> lock(mtx_);
            queue_.push(v);
        }

        cond_.notify_one();
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mtx_);

        return queue_.empty();
    }

   private:
    std::queue<T> queue_;

    std::mutex mtx_;
    std::condition_variable cond_;
};

void arg_parse(int argc, char *argv[], std::string &from) {
    int c;

    int option_index = 0;

    struct option long_options[] = {
        {0, 0, 0, 0},
    };

    while ((c = getopt_long(argc, argv, "", long_options, &option_index)) !=
           -1) {
    }

    // TODO(y1r): Support multiple files
    assert(optind == (argc - 1));

    from = std::string(argv[optind++]);
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

void copy_file_using_direct_io(const std::string &from,
                               size_t iterations, int mpi_rank) {
    ConcurrentQueue<void *> memory_pool;
    for (int i = 0; i < N_OF_BUFFERS; i++) {
        // C11 function
        void *buffer = aligned_alloc(512, BLOCKSIZE);

        memory_pool.push(buffer);
    }

    ConcurrentQueue<void *> completed_read;
    ConcurrentQueue<void *> completed_bcast;

    std::thread read_thread([
                                // Input
                                iterations, mpi_rank, &memory_pool, from,
                                // Output
                                &completed_read] {
        int read_fd = -1;

        if (mpi_rank == ROOT) {
            read_fd = open(from.c_str(), O_RDONLY);
            assert(read_fd != -1);
        }

        for (size_t i = 0; i < iterations; i++) {
            void *buffer = memory_pool.pop();
            if (mpi_rank == ROOT) {
                read(read_fd, buffer, BLOCKSIZE);
            }

            completed_read.push(buffer);
        }

        completed_read.push(nullptr);
        if (read_fd != -1) close(read_fd);
    });

    std::thread bcast_thread(
        [
            // Input
            &completed_read, mpi_rank,
            // Output
            &completed_bcast] {
            while (true) {
                void *buffer = completed_read.pop();
                if (buffer == nullptr) {
                    break;
                }
                MPI_Bcast(buffer, BLOCKSIZE, MPI_BYTE, ROOT, MPI_COMM_WORLD);
                completed_bcast.push(buffer);
            }

        completed_bcast.push(nullptr);
        });

    std::thread write_thread(
        [
            // Input
            &completed_bcast, mpi_rank,
            // Output
            &memory_pool] {
            int write_fd = STDOUT_FILENO;

            while (true) {
                void *buffer = completed_bcast.pop();
                if (buffer == nullptr) {
                    break;
                }
                write(write_fd, buffer, BLOCKSIZE);
                memory_pool.push(buffer);
            }
        });

    read_thread.join();
    bcast_thread.join();
    write_thread.join();

    while (not memory_pool.empty()) {
        free(memory_pool.pop());
    }
}

void copy_file_with_offset(const std::string &from,
                           size_t offset, size_t bytes,
                           int mpi_rank) {
    if (bytes == 0) return;

    int read_fd = -1;
    int write_fd = STDOUT_FILENO;

    void *buffer = malloc(bytes);

    if (mpi_rank == ROOT) {
        read_fd = open(from.c_str(), O_RDONLY);
        assert(read_fd != -1);
    }

    if (mpi_rank == ROOT) {
        pread(read_fd, buffer, bytes, offset);
    }

    MPI_Bcast(buffer, bytes, MPI_BYTE, ROOT, MPI_COMM_WORLD);

    write(write_fd, buffer, bytes);

    free(buffer);

    if (read_fd != -1) close(read_fd);
}

int main(int argc, char *argv[]) {
    std::string from;

    arg_parse(argc, argv, from);

    int mpi_rank, mpi_size;
    init_mpi(&argc, &argv, mpi_rank, mpi_size);

    size_t filesize = 0;
    if (mpi_rank == ROOT) filesize = get_filesize(from);
    MPI_Bcast(&filesize, sizeof(filesize), MPI_BYTE, ROOT, MPI_COMM_WORLD);

    size_t direct_io_iterations = filesize / BLOCKSIZE;

    copy_file_using_direct_io(from, direct_io_iterations, mpi_rank);

    size_t transferred = direct_io_iterations * BLOCKSIZE;

    copy_file_with_offset(from, transferred, filesize - transferred, mpi_rank);

    MPI_Finalize();

    return 0;
}
