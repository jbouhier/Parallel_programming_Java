package edu.coursera.distributed;

import edu.coursera.distributed.util.MPI;
import edu.coursera.distributed.util.MPI.MPIException;

/**
 * A wrapper class for a parallel, MPI-based matrix multiply implementation.
 */
public class MatrixMult {
    /**
     * Matrix multiply using MPI to express SPMD parallelism.
     *
     * @param a Input matrix
     * @param b Input matrix
     * @param c Output matrix
     * @param mpi MPI object supporting MPI APIs
     * @throws MPIException On MPI error
     */
    public static void parallelMatrixMultiply(Matrix a, Matrix b, Matrix c, final MPI mpi) throws MPIException {

        final int myrank = mpi.MPI_Comm_rank(mpi.MPI_COMM_WORLD);
        final int size   = mpi.MPI_Comm_size(mpi.MPI_COMM_WORLD);

        final int nrows = c.getNRows();
        final int rowChunk = (nrows + size + 1) / size;
        final int startRow = myrank * rowChunk;
        int endRow = (myrank + 1) * rowChunk;
        if (endRow > nrows) endRow = nrows;

        // Broadcast matrices A and B to all ranks (nodes) for simplicity
        mpi.MPI_Bcast(a.getValues(), 0, a.getNRows() * a.getNCols(), 0, mpi.MPI_COMM_WORLD);
        mpi.MPI_Bcast(b.getValues(), 0, b.getNRows() * b.getNCols(), 0, mpi.MPI_COMM_WORLD);

        for (int i = startRow; i < endRow; i++) {
            for (int j = 0; j < c.getNCols(); j++) {
                c.set(i, j, 0.0);
                for (int k = 0; k < b.getNRows(); k++) {
                    c.incr(i, j, a.get(i, k) * b.get(k, j));
                }
            }
        }

        // Assembling result in rank 0
        if (myrank == 0) {
            MPI.MPI_Request[] requests = new MPI.MPI_Request[size - 1];

            for (int i = 1; i < size; i++) {
                final int rankStartRow = i * rowChunk;
                int rankEndRow = (i + 1) * rowChunk;
                if (rankEndRow > nrows) rankEndRow = nrows;

                final int rowOffset = rankStartRow * c.getNCols();
                final int nElements = (rankEndRow - rankStartRow) * c.getNCols();
                requests[i - 1] = mpi.MPI_Irecv(c.getValues(), rowOffset, nElements, i, i, mpi.MPI_COMM_WORLD);
            }
            mpi.MPI_Waitall(requests);
        } else {
            mpi.MPI_Send(
                c.getValues(),
                startRow * c.getNCols(),
                (endRow - startRow) * c.getNCols(),
                0,
                myrank,
                mpi.MPI_COMM_WORLD
            );
        }
    }
}
