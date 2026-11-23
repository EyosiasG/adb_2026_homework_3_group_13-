import java.io.BufferedOutputStream;
import java.io.FileDescriptor;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 
 * @author Martin Schaeler martin.schaeler@plus.ac.at
 * 
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
 *
 * Homework 3 (Group 13):
 * Throughput benchmark with a realistic transaction mix.
 *
 * Every transaction (Blocker) dices p in [0,1] once:
 *   p <  0.6  read-only multi-point query (num_queries_per_task point reads, prints the sum)
 *   p <  0.7  insert of one new tuple
 *   p <  0.8  full table scan (prints sum(data_value))
 *   p >= 0.8  write: num_queries_per_task single-tuple updates
 *
 * Continuous stream: a fixed pool of num_taks transactions. Each worker thread takes the next
 * transaction from the pool, runs it until it commits, and takes the next one; after the last one
 * the pool starts again from the beginning. This is a closed system: exactly num_threads
 * transactions are in flight at any time, so new and committed transactions are in equilibrium.
 * The first warmup_ms are not measured, then commits are counted for duration_ms.
 *
 * Writes can still deadlock. They are restarted with backoff, and after escalate_after aborts a
 * write runs exclusively among the writers (same fair gate as our Homework 2 NoLiveLocks), so every transaction
 * commits eventually. Reads, scans and inserts take no conflicting row locks and never use the gate.
 *
 * Usage: java Blocking [threads|serial] [seconds] [mode]
 *   serial = trivial serial execution: one worker, one transaction after the other
 *   mode = baseline | optimized | comma separated list of single optimizations:
 *     buffer  buffered console output instead of a flush per printed line
 *     async   synchronous_commit = off, a commit does not wait for the WAL flush to disk
 *     reuse   prepare each statement once per connection (server side prepared plans)
 *     batch   send the update statements of a write as one JDBC batch (same order), needs reuse
 *     index   extra index on data_value, so sum(data_value) can use an index only scan
 *   optimized = buffer,async,reuse,batch
 */
public class Blocking {
    public static int num_taks = 1000;
    public static int num_queries_per_task = 3;
    public static int num_threads = 4;
    public static int duration_ms = 30000;
    public static int warmup_ms = 2000;
    public static String mode = "baseline";
    static final Set<String> opts = new HashSet<>();

    static boolean opt(String o) {
        return opts.contains(o);
    }

    enum Type { POINT_READ, INSERT, SCAN, WRITE }

    static final String SELECT_POINT = "SELECT data_value FROM blocking_data WHERE data_id = ?";
    static final String INSERT_ROW = "INSERT INTO blocking_data VALUES (?, ?, ?)";
    static final String SELECT_SUM = "SELECT sum(data_value) FROM blocking_data";
    static final String UPDATE_ROW = "UPDATE blocking_data SET data_value = ?, modified_by = ? WHERE data_id = ?";

    final Random rand = new Random(123456);
    final int num_tuple = 50000;
    final double max_value = 50.0;
    final int escalate_after = 3;
    static final long BASE_BACKOFF_MS = 5;
    static final long MAX_BACKOFF_MS = 200;

    final List<Blocker> pool = new ArrayList<>();
    final AtomicInteger next_task = new AtomicInteger(0);
    final AtomicInteger next_insert_id = new AtomicInteger(num_tuple);
    final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);

    volatile boolean measuring = false;
    volatile boolean stop = false;
    final AtomicLong committed_total = new AtomicLong(0);
    final AtomicLong[] commits = counters();
    final AtomicLong[] nanos = counters();
    final AtomicLong[] retries = counters();
    final AtomicInteger max_retries = new AtomicInteger(0);
    final AtomicInteger escalated = new AtomicInteger(0);

    PrintStream out = System.out;

    static AtomicLong[] counters() {
        AtomicLong[] a = new AtomicLong[Type.values().length];
        for (int i = 0; i < a.length; i++) a[i] = new AtomicLong(0);
        return a;
    }

    void executeSQL(String sql, Connection con) {
        try (Statement stmt = con.createStatement()) {
            stmt.execute(sql);
            System.out.println("Executing " + sql);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    /** Drops and reloads the table, so tuples inserted by a previous run are gone. Not measured. */
    void cerateDB() {
        try (Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL)) {
            executeSQL("DROP TABLE IF EXISTS blocking_data;", con);
            executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);", con);
            con.setAutoCommit(false);
            try (PreparedStatement ps = con.prepareStatement(INSERT_ROW)) {
                for (int i = 0; i < num_tuple; i++) {
                    ps.setInt(1, i);
                    ps.setDouble(2, rand.nextDouble() * max_value);
                    ps.setInt(3, 0);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            con.commit();
            con.setAutoCommit(true);
            if (opt("index")) executeSQL("CREATE INDEX blocking_data_value ON blocking_data (data_value);", con);
            executeSQL("VACUUM ANALYZE blocking_data;", con);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) num_threads = args[0].equalsIgnoreCase("serial") ? 1 : Integer.parseInt(args[0]);
        if (args.length > 1) duration_ms = Integer.parseInt(args[1]) * 1000;
        if (args.length > 2) mode = args[2].toLowerCase();
        opts.clear();
        if (mode.equals("optimized")) {
            opts.addAll(List.of("buffer", "async", "reuse", "batch"));
        } else if (!mode.equals("baseline")) {
            opts.addAll(List.of(mode.split(",")));
        }

        Blocking b = new Blocking();
        b.cerateDB();
        for (int task = 0; task < num_taks; task++) {
            b.pool.add(b.new Blocker(task));
        }
        if (opt("buffer")) {
            b.out = new PrintStream(new BufferedOutputStream(new FileOutputStream(FileDescriptor.out), 1 << 16), false);
        }

        System.out.println("[START] " + (num_threads == 1 ? "serial execution" : "concurrent execution")
                + " threads=" + num_threads + " mode=" + mode + " pool=" + num_taks + " " + b.mix());

        List<Thread> workers = new ArrayList<>();
        for (int i = 0; i < num_threads; i++) {
            Thread t = new Thread(b.new Worker(), "worker-" + i);
            workers.add(t);
            t.start();
        }

        long start = System.currentTimeMillis();
        Thread.sleep(warmup_ms);
        long base = b.committed_total.get();
        b.measuring = true;
        long measure_start = System.nanoTime();
        long last = base;
        for (int s = 1; s * 1000L <= duration_ms; s++) {
            Thread.sleep(1000);
            long now = b.committed_total.get();
            b.out.flush();
            System.out.println("[" + s + "s] " + (now - last) + " commits/s");
            last = now;
        }
        b.measuring = false;
        double measured_s = (System.nanoTime() - measure_start) / 1e9;
        b.stop = true;
        for (Thread t : workers) t.join();
        b.out.flush();

        b.report(measured_s, (System.currentTimeMillis() - start) / 1000.0);
    }

    String mix() {
        int[] n = new int[Type.values().length];
        for (Blocker t : pool) n[t.type.ordinal()]++;
        StringBuilder sb = new StringBuilder("mix:");
        for (Type ty : Type.values()) sb.append(' ').append(ty).append('=').append(n[ty.ordinal()]);
        return sb.toString();
    }

    void report(double measured_s, double wall_s) {
        long total = 0, total_nanos = 0;
        for (Type ty : Type.values()) {
            total += commits[ty.ordinal()].get();
            total_nanos += nanos[ty.ordinal()].get();
        }
        System.out.println();
        System.out.printf("[DONE] threads=%d mode=%s measured=%.1fs committed=%d throughput=%.1f tx/s%n",
                num_threads, mode, measured_s, total, total / measured_s);
        System.out.printf("%-11s %8s %10s %12s %9s%n", "type", "commits", "avg ms", "time share", "retries");
        for (Type ty : Type.values()) {
            int i = ty.ordinal();
            long c = commits[i].get();
            System.out.printf("%-11s %8d %10.3f %11.1f%% %9d%n", ty, c,
                    c == 0 ? 0.0 : nanos[i].get() / 1e6 / c,
                    total_nanos == 0 ? 0.0 : 100.0 * nanos[i].get() / total_nanos, retries[i].get());
        }
        System.out.println("max retries of one transaction: " + max_retries.get()
                + ", escalated writes: " + escalated.get());
        System.out.printf("CSV,%d,%s,%.2f,%d,%.1f%n", num_threads, mode.replace(',', '+'),
                measured_s, total, total / measured_s);
    }

    /** One worker = one hardware thread with its own connection, running transactions back to back. */
    class Worker implements Runnable {
        @Override
        public void run() {
            Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
            try {
                con.setAutoCommit(false);
                con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                if (opt("async")) {
                    try (Statement s = con.createStatement()) {
                        s.execute("SET synchronous_commit = off");
                    }
                    con.commit();
                }
                Statements st = opt("reuse") ? new Statements(con) : null;
                while (!stop) {
                    Blocker t = pool.get(Math.floorMod(next_task.getAndIncrement(), num_taks));
                    long t0 = System.nanoTime();
                    int r = t.execute(con, st);
                    if (r < 0) continue;
                    committed_total.incrementAndGet();
                    if (measuring) {
                        int i = t.type.ordinal();
                        commits[i].incrementAndGet();
                        nanos[i].addAndGet(System.nanoTime() - t0);
                        retries[i].addAndGet(r);
                        max_retries.accumulateAndGet(r, Math::max);
                    }
                }
            } catch (SQLException e) {
                System.err.println(Thread.currentThread().getName() + " " + e.getMessage());
            } finally {
                try {
                    con.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    /** Prepared once per connection (optimized mode only). */
    static class Statements {
        final PreparedStatement point, insert, sum, update;

        Statements(Connection con) throws SQLException {
            point = con.prepareStatement(SELECT_POINT);
            insert = con.prepareStatement(INSERT_ROW);
            sum = con.prepareStatement(SELECT_SUM);
            update = con.prepareStatement(UPDATE_ROW);
        }
    }

    public class Blocker {
        final int task;
        final Type type;
        final int[] ids;
        final double[] new_value;

        public Blocker(int id) {
            this.task = id;
            double p = rand.nextDouble();
            type = p < 0.6 ? Type.POINT_READ : p < 0.7 ? Type.INSERT : p < 0.8 ? Type.SCAN : Type.WRITE;
            ids = new int[num_queries_per_task];
            new_value = new double[num_queries_per_task];
            Set<Integer> used = new HashSet<>();
            for (int q = 0; q < num_queries_per_task; q++) {
                int tuple;
                do {
                    tuple = rand.nextInt(num_tuple);
                } while (!used.add(tuple));
                ids[q] = tuple;
                new_value[q] = rand.nextDouble() * max_value;
            }
        }

        PreparedStatement prepare(Connection con, Statements st, String sql) throws SQLException {
            if (st == null) {
                return con.prepareStatement(sql, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            }
            switch (sql) {
                case SELECT_POINT: return st.point;
                case INSERT_ROW: return st.insert;
                case SELECT_SUM: return st.sum;
                default: return st.update;
            }
        }

        void release(PreparedStatement ps, Statements st) throws SQLException {
            if (st == null) ps.close();
        }

        /** Runs this transaction until it commits. Returns the number of retries, or -1 on a fatal error. */
        int execute(Connection con, Statements st) {
            int attempts = 0;
            int insert_id = type == Type.INSERT ? next_insert_id.getAndIncrement() : -1;
            while (true) {
                Lock lock = null;
                if (type == Type.WRITE) {
                    lock = attempts >= escalate_after ? gate.writeLock() : gate.readLock();
                    lock.lock();
                }
                String result;
                try {
                    result = body(con, st, insert_id);
                    con.commit();
                } catch (SQLException e) {
                    try {
                        con.rollback();
                    } catch (SQLException ignore) {
                    }
                    String state = e.getSQLState();
                    for (SQLException n = e; n != null && !retryable(state); n = n.getNextException()) {
                        state = n.getSQLState();
                    }
                    if (!retryable(state)) {
                        System.err.println("Task " + task + " fatal (" + state + "): " + e.getMessage());
                        return -1;
                    }
                    attempts++;
                    result = null;
                } finally {
                    if (lock != null) lock.unlock();
                }
                if (result != null) {
                    if (type == Type.WRITE && attempts >= escalate_after) escalated.incrementAndGet();
                    out.println("Done task " + task + " " + result + (attempts > 0 ? " after " + attempts + " retries" : ""));
                    return attempts;
                }
                long cap = Math.min(MAX_BACKOFF_MS, BASE_BACKOFF_MS << Math.min(attempts, 10));
                try {
                    Thread.sleep(ThreadLocalRandom.current().nextLong(cap + 1));
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    return -1;
                }
            }
        }

        boolean retryable(String state) {
            return "40P01".equals(state) || "40001".equals(state);
        }

        String body(Connection con, Statements st, int insert_id) throws SQLException {
            PreparedStatement ps;
            switch (type) {
                case POINT_READ: {
                    ps = prepare(con, st, SELECT_POINT);
                    double sum = 0;
                    for (int q = 0; q < num_queries_per_task; q++) {
                        ps.setInt(1, ids[q]);
                        try (ResultSet rs = ps.executeQuery()) {
                            if (rs.next()) sum += rs.getDouble(1);
                        }
                    }
                    release(ps, st);
                    return "POINT_READ sum=" + sum;
                }
                case INSERT: {
                    ps = prepare(con, st, INSERT_ROW);
                    ps.setInt(1, insert_id);
                    ps.setDouble(2, new_value[0]);
                    ps.setInt(3, task);
                    ps.executeUpdate();
                    release(ps, st);
                    return "INSERT id=" + insert_id;
                }
                case SCAN: {
                    ps = prepare(con, st, SELECT_SUM);
                    double sum;
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        sum = rs.getDouble(1);
                    }
                    release(ps, st);
                    return "SCAN sum=" + sum;
                }
                default: {
                    ps = prepare(con, st, UPDATE_ROW);
                    for (int q = 0; q < num_queries_per_task; q++) {
                        ps.setDouble(1, new_value[q]);
                        ps.setInt(2, task);
                        ps.setInt(3, ids[q]);
                        if (st != null && opt("batch")) {
                            ps.addBatch();
                        } else {
                            ps.executeUpdate();
                        }
                    }
                    if (st != null && opt("batch")) ps.executeBatch();// same statements in the same order, one round trip
                    release(ps, st);
                    return "WRITE";
                }
            }
        }
    }
}
