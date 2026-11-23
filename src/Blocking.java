import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
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
*/

public class Blocking {
	public static int num_taks=30;
	public static int num_queries_per_task = 3;

	// ===== Benchmark settings (change them here) =====
	public static int num_threads = 4;          // PART 1 - EXERCISE 3: try 1, 2, 4, 8, 16. 1 = serial execution (PART 2 - EXERCISE 1)
	public static int warmup_seconds = 2;       // not measured: connections are opened and the JVM warms up
	public static int duration_seconds = 30;    // measured time

	Connection con;
	Random rand = new Random(123456);
	int num_tuple = 50000;
	double max_value = 50.0f;
	AtomicInteger tasks_finished;
	AtomicInteger deadlocks = new AtomicInteger(0); // counts deadlock retries
	AtomicInteger max_retries = new AtomicInteger(0);

	// statistics per transaction type, for the report at the end
	static final String[] TYPES = {"READ", "INSERT", "SCAN", "WRITE"};
	AtomicLong[] commits = new AtomicLong[4];
	AtomicLong[] nanos = new AtomicLong[4];
	AtomicLong[] retries = new AtomicLong[4];

	// writes can still deadlock: after 3 aborts a write runs alone among the writers, so it cannot starve
	final ReentrantReadWriteLock gate = new ReentrantReadWriteLock(true);

	// FIX: opening a new TCP connection per transaction ran out of local ports on macOS
	// ("BindException: Can't assign requested address"). Each worker thread now opens ONE
	// connection and reuses it. Still a separate connection per concurrently running transaction.
	final ThreadLocal<Connection> threadConnection = ThreadLocal.withInitial(this::openConnection);
	
	public Blocking(){
		tasks_finished = new AtomicInteger(0);  //AtomicInteger provides thread-safe operations, while updating the same int value
		for (int i = 0; i < 4; i++) {
			commits[i] = new AtomicLong();
			nanos[i] = new AtomicLong();
			retries[i] = new AtomicLong();
		}
	}
	
	void executeSQL(String sql, Connection con){
		java.sql.Statement stmt;
		try {
			stmt = con.createStatement();
			stmt.execute(sql);
			System.out.println("Executing "+sql);
			stmt.close();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	// the connection settings are done ONCE per thread instead of in every transaction
	Connection openConnection() {
		Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
		try {
			con.setAutoCommit(false);
			con.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return con;
	}
	
	// PART 1 - EXERCISE 1c: the table is dropped and reloaded at every start, so inserted tuples are removed
	void cerateDB(){	
		try {
			Connection con = ConnectionFactory.getDefaultParameterConnection(ConnectionFactory.POSTGRESQL);
			executeSQL("DROP TABLE IF EXISTS blocking_data;",con);
			executeSQL("CREATE TABLE blocking_data ( data_id integer primary key, data_value float, modified_by integer);",con);
			
			PreparedStatement ps;
			con.setAutoCommit(false); //Setting autoCommit to false allows multiple database operations to be grouped into one transaction.
			ps = con.prepareStatement("INSERT INTO blocking_data VALUES (?, ?, ?)",ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
			
			for(int i=0;i<num_tuple;i++){ // num_tuple
				ps.setInt(1, i);
				ps.setDouble(2,rand.nextDouble()*max_value);
				ps.setInt(3, 0);
				ps.addBatch();
				ps.clearParameters();
			}

			int[] results = ps.executeBatch();
			con.commit();
			con.setAutoCommit(true);
			executeSQL("VACUUM ANALYZE blocking_data;", con); // fresh statistics for the query planner
			con.close();
			System.out.println("Loaded " + results.length + " tuples");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
//	public static void main(String[] args){
//		//Create DB
//		Blocking b = new Blocking();
//		b.cerateDB();
//
//		//Create task and their queries
//		ArrayList<Blocking.Blocker> my_tasks = new ArrayList<Blocking.Blocker>(num_taks);
//
//		for(int task=0;task<num_taks;task++){
//			my_tasks.add(b.new Blocker(task)); //This creates 30 Blocker objects with IDs from 0 to 29.
//		}
//
//		//Execute tasks
//		ThreadPoolExecutor executor =
//				  (ThreadPoolExecutor) Executors.newFixedThreadPool(4);//XXX - Do not change the number of threads used
//		double start, stop;
//		start = System.currentTimeMillis();
//		for(Blocker task : my_tasks) {
//			executor.execute(task);
//		}
//		executor.shutdown();
//
//		//wait for the tasks to finish
//		while(!executor.isTerminated()) {
//			try {
//				Thread.sleep(100); //Keep waiting every 0.1s until the executor is completely done.
//			} catch (InterruptedException e) {
//				e.printStackTrace();
//			}
//		}
//		stop = System.currentTimeMillis();
//		System.out.println("[DONE] Task execution after roughly "+(stop-start)+" ms finished: "+b.tasks_finished+" of "+num_taks);
//	}
 
    // PART 1 - EXERCISE 2: Continuous stream of transactions
    // Each worker thread starts a NEW transaction as soon as its previous one commits.
    // Equilibrium: #running transactions == #threads 
    // PART 2 - EXERCISE 1: num_threads = 1 is the serial execution baseline.
    public static void main(String[] args) throws InterruptedException {
        if (args.length > 0) num_threads = Integer.parseInt(args[0]);

        Blocking b = new Blocking();
        b.cerateDB();

        int workers = num_threads;
        long durationNanos = TimeUnit.SECONDS.toNanos(duration_seconds);
        AtomicInteger nextTaskId = new AtomicInteger(0);

        System.out.println("[START] " + (workers == 1 ? "serial" : "concurrent") + " execution with " + workers + " thread(s)");

        ExecutorService executor = Executors.newFixedThreadPool(workers);
        long start = System.nanoTime();

        for (int worker = 0; worker < workers; worker++) {
            executor.execute(() -> {
                while (System.nanoTime() - start < durationNanos) {
                    int taskId = nextTaskId.getAndIncrement();
                    b.new Blocker(taskId).run();
                }
                try { b.threadConnection.get().close(); } catch (SQLException ignored) { } // close this thread's connection at the end
            });
        }
        executor.shutdown();

        // print the commits of every second while the workers are running
        int last = 0;
        int second = 0;
        while (!executor.awaitTermination(1, TimeUnit.SECONDS)) {
            int now = b.tasks_finished.get();
            System.out.println("[" + (++second) + "s] " + (now - last) + " commits/s");
            last = now;
        }

        // PART 1 - EXERCISE 3: measure throughput (committed transactions per second)
        double elapsedSeconds = (System.nanoTime() - start) / 1_000_000_000.0;
        b.report(workers, nextTaskId.get(), elapsedSeconds);
    }

    void report(int workers, int started, double elapsedSeconds) {
        int committed = tasks_finished.get();
        long total_nanos = 0;
        for (AtomicLong n : nanos) total_nanos += n.get();

        System.out.println();
        System.out.println("Worker threads: " + workers);
        System.out.println("Transactions started: " + started);
        System.out.println("Transactions committed: " + committed);
        System.out.println("Deadlock retries: " + deadlocks.get() + " (max retries of one transaction: " + max_retries.get() + ")");
        System.out.println("Elapsed time: " + Math.round(elapsedSeconds * 1000) / 1000.0 + " seconds");
        System.out.println("Throughput: " + Math.round(committed / elapsedSeconds) + " commits/second");

        // per transaction type: how many committed, how long one took on average, and its share of the total time
        System.out.println();
        for (int i = 0; i < 4; i++) {
            long c = commits[i].get();
            double avg_ms = c == 0 ? 0 : nanos[i].get() / 1_000_000.0 / c;
            double share = total_nanos == 0 ? 0 : 100.0 * nanos[i].get() / total_nanos;
            System.out.println(TYPES[i] + ": " + c + " commits, avg " + Math.round(avg_ms * 1000) / 1000.0 + " ms, "
                    + Math.round(share) + "% of the time, " + retries[i].get() + " retries");
        }
    }
	
	/**
	 * One transaction of the new mix. Its type is decided by p.
	 */
	public class Blocker implements Runnable  {
		final int task;
        // PART 1 - EXERCISE 1a: random value p in [0,1] decides the transaction type
        final double p;
        final int type; // 0 = point read, 1 = insert, 2 = scan, 3 = write
        // rows and new values are fixed per transaction, so a retry repeats the SAME transaction
        final int[] ids;
        final double[] new_values;
        boolean failed = false; // a real SQL error (not a deadlock): not counted in the statistics
		
		public Blocker(int id){
			this.task = id;
            // PART 1 - EXERCISE 1a: dice p i.i.d. for each transaction
            ThreadLocalRandom random = ThreadLocalRandom.current();
            this.p = random.nextDouble();
            this.type = p < 0.6 ? 0 : p < 0.7 ? 1 : p < 0.8 ? 2 : 3;
            this.ids = random.ints(0, num_tuple).distinct().limit(num_queries_per_task).toArray();
            this.new_values = random.doubles(num_queries_per_task, 0, max_value).toArray();
		}
		
		// PART 2 (requirement): every transaction must eventually commit
		// -> a deadlock victim is rolled back and the SAME transaction is restarted
		@Override
		public void run() {
			long start = System.nanoTime();
			int attempts = 0;
			while (!attempt(attempts)) {
				attempts++;
				deadlocks.incrementAndGet();
				// random backoff (5, 10, 20 ... max 200 ms), so the same transactions do not collide again at once
				try {
					Thread.sleep(random_backoff(attempts));
				} catch (InterruptedException e) {
					return;
				}
			}
			if (failed) return;
			commits[type].incrementAndGet();
			nanos[type].addAndGet(System.nanoTime() - start);
			retries[type].addAndGet(attempts);
			max_retries.accumulateAndGet(attempts, Math::max);
		}

		long random_backoff(int attempts) {
			long cap = Math.min(200, 5L << Math.min(attempts, 10));
			return ThreadLocalRandom.current().nextLong(cap + 1);
		}

		/** @return true if finished, false if aborted by a deadlock and must be retried */
		boolean attempt(int attempts) {
			// FIX (connection reuse): one connection per worker THREAD, reused for all its transactions.
			Connection con = threadConnection.get();

			// only writes take locks that can deadlock. After 3 aborts the write gets the write lock and runs alone
			Lock lock = null;
			if (type == 3) {
				lock = attempts >= 3 ? gate.writeLock() : gate.readLock();
				lock.lock();
			}

			try {
                // PART 1 - EXERCISE 1b: p < 0.6 -> read-only multi-point query (3 distinct tuples, print sum)
                if (type == 0) {
                    double sum = 0.0;
                    String sql = "SELECT data_value FROM blocking_data WHERE data_id = ?";

                    try (PreparedStatement read = con.prepareStatement(sql)) {
                        for (int rowId : ids) {
                            read.setInt(1, rowId);

                            try (ResultSet result = read.executeQuery()) {
                                if (!result.next()) {
                                    throw new SQLException("Row " + rowId + " was not found");
                                }
                                sum += result.getDouble("data_value");
                            }
                        }
                    }

                    con.commit();
                    tasks_finished.incrementAndGet();

                    System.out.println("Task " + task + " read sum: " + sum);
                    return true;
                }

                // PART 1 - EXERCISE 1c: 0.6 <= p < 0.7 -> insert a new tuple
                // task ids are unique, so num_tuple + task is a new primary key
                if (type == 1) {
                    int newId = num_tuple + task;
                    String sql = "INSERT INTO blocking_data (data_id, data_value, modified_by) "
                            + "VALUES (?, ?, ?)";

                    try (PreparedStatement insert = con.prepareStatement(sql)) {
                        insert.setInt(1, newId);
                        insert.setDouble(2, new_values[0]);
                        insert.setInt(3, task);
                        insert.executeUpdate();
                    }

                    con.commit();
                    tasks_finished.incrementAndGet();

                    System.out.println("Task " + task + " inserted tuple " + newId);
                    return true;
                }

                // PART 1 - EXERCISE 1d: 0.7 <= p < 0.8 -> full table scan, print sum
                if (type == 2) {
                    String sql = "SELECT SUM(data_value) FROM blocking_data";
                    double sum;

                    try (PreparedStatement scan = con.prepareStatement(sql);
                         ResultSet result = scan.executeQuery()) {
                        result.next();
                        sum = result.getDouble(1);
                    }

                    con.commit();
                    tasks_finished.incrementAndGet();

                    System.out.println("Task " + task + " table sum: " + sum);
                    return true;
                }

                // PART 1 - EXERCISE 1e: p >= 0.8 -> write query, 3 single-tuple updates (order is never changed)
                String sql = "UPDATE blocking_data "
                        + "SET data_value = ?, modified_by = ? "
                        + "WHERE data_id = ?";

                try (PreparedStatement update = con.prepareStatement(sql)) {
                    for (int q = 0; q < ids.length; q++) {
                        update.setDouble(1, new_values[q]);
                        update.setInt(2, task);
                        update.setInt(3, ids[q]);
                        update.executeUpdate();
                    }
                }

				con.commit();
				int num_finished = tasks_finished.incrementAndGet();
				System.out.println("Done task "+task+". I am finisher number "+num_finished);
				return true;
			} catch (SQLException e) {
				// Old code only printed the error: the transaction was lost and the connection leaked.
				try { con.rollback(); } catch (SQLException ignored) { }
				if (isDeadlock(e)) {   // deadlock victim -> retry
					return false;
				}
				System.err.println("Task "+task+" "+e.getMessage());
				failed = true;
				return true;                           // other errors: don't loop forever
			} finally {
				if (lock != null) lock.unlock();
			}
		}

		// 40P01 = deadlock, 40001 = serialization failure
		boolean isDeadlock(SQLException e) {
			for (SQLException ex = e; ex != null; ex = ex.getNextException()) {
				if ("40P01".equals(ex.getSQLState()) || "40001".equals(ex.getSQLState())) {
					return true;
				}
			}
			return false;
		}
	}
}
