import tarfile
import os
import argparse
import io
from typing import Optional, Union

from mpi4py import MPI

KB = 1024
MB = 1024 * KB

# Extract buffer size
#TAR_BUF_SIZE = 128 * KB

# MPI_Bcast buffer size
BCAST_BUF_SIZE = 1 * MB

# File read buffer size
READ_BUF_SIZE = 4 * MB


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('file_from')
    parser.add_argument('file_to')
    parser.add_argument('--untar', action='store_true', default=False)

    args = parser.parse_args()

    try:
        stream = MPIFileStream(args.file_from)

        if args.untar:
            t = tarfile.open(mode='r|', fileobj=stream)
            t.extractall(path=args.file_to)
        else:
            assert not os.path.exists(args.file_to)
            with open(args.file_to, 'wb') as f:
                while True:
                    b = stream.read(BCAST_BUF_SIZE)
                    if len(b) == 0:
                        break
                    f.write(b)

    except FileNotFoundError:
        print(f"File {args.file_from} is not found")
        MPI.Finalize()
        exit()


class MPIFileStream(io.RawIOBase):
    def __init__(self, filename: str):
        self.rank = MPI.COMM_WORLD.rank
        self.reader = False
        self.i = 0

        self.filesize = -1
        if self.rank == 0:
            try:
                assert os.path.isfile(filename)
                self.filesize = os.path.getsize(filename)
            except OSError as e:
                print(e)
                self.filesize = -1

        self.filesize = MPI.COMM_WORLD.bcast(self.filesize)
        if self.filesize == -1:
            raise FileNotFoundError

        if self.rank == 0:
            file_stream = io.FileIO(filename, 'rb')
            self.actual_file_stream = io.BufferedReader(file_stream, READ_BUF_SIZE)
            self.reader = True

    def readall(self) -> bytes:
        raise io.UnsupportedOperation

    def write(self, __b: Union[bytes, bytearray]) -> Optional[int]:
        raise io.UnsupportedOperation

    def read(self, __size: int = ...) -> Optional[bytes]:
        this_load = min(self.filesize - self.i, __size)

        if self.reader:
            buf = self.actual_file_stream.read(this_load)
            assert len(buf) == this_load
        else:
            buf = bytearray(this_load)

        if len(buf) > 0:
            MPI.COMM_WORLD.Bcast(buf)

        self.i += this_load

        return buf


if __name__ == '__main__':
    main()
