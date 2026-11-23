# adb_2026_task_2
Task 2 of Winter Semester 2026/27 advanced database proseminar.
Task 3 get's the same starter code.

## Homework 3 – Homework 2 revisited

`src/Blocking.java` runs a continuous stream of transactions with the mix from the task:

| p | share | transaction |
|---|---|---|
| < 0.6 | 60 % | 3 point reads `SELECT data_value ... WHERE data_id = ?` on distinct tuples, prints the sum |
| < 0.7 | 10 % | insert of one new tuple (ids from 50000 upwards) |
| < 0.8 | 10 % | full scan `SELECT sum(data_value) FROM blocking_data`, prints the sum |
| ≥ 0.8 | 20 % | write: 3 single-tuple updates `UPDATE ... WHERE data_id = ?` |

* **Stream / equilibrium:** a pool of 1000 transactions. Every worker thread runs one transaction until it
  commits and then takes the next one; at the end of the pool it starts over. This is a closed system, so
  exactly `threads` transactions are in flight and new and committed transactions are always in balance.
  The commits per second are printed every second and stay constant after the 2 s warm-up.
* **Every transaction commits eventually:** deadlocked writes are restarted with backoff and after 3 aborts
  run exclusively among the writers (gate from `NoLiveLocks`). Reads, scans and inserts never wait for it.
* The table is dropped and reloaded at every start, so inserted tuples of an earlier run are removed.
  Loading is not measured.

Run (from the project root, PostgreSQL running locally):

```
javac -d out_cli -cp lib/postgresql-42.7.14.jar src/*.java
java -cp "out_cli;lib/postgresql-42.7.14.jar" Blocking 16 30 optimized       # threads, seconds, mode
java -cp "out_cli;lib/postgresql-42.7.14.jar" Blocking serial 30 baseline    # serial execution
```

Modes: `baseline`, `optimized`, or a comma separated list of `buffer`, `async`, `reuse`, `batch`, `index`
(see the class comment). `optimized` = `buffer,async,reuse,batch`.
