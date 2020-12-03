package ai.brace.transaction_test;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Transaction;
import io.vertx.sqlclient.Tuple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.intellij.lang.annotations.Language;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

public class MainVerticle extends AbstractVerticle
{
  private static final Logger logger = LogManager.getLogger();

  private static final int PORT = 5432;
  private static final String APPLICATION_NAME = "transaction-test";
  private static final String DATABASE_NAME = "postgres";
  private static final String DATABASE_USER = "root";
  private static final String PASSWORD = "";
  private static final String HOST = "localhost";
  private static final int MAX_SQL_POOL_SIZE = 10;
  private static final int MAX_WAIT_QUEUE_SIZE = 4500;
  private static final boolean CACHE_PREPARED_STATEMENTS = false;

  @Language("PostgreSQL") private static final String STATUS_QUERY = """
    select pid, state
    from pg_stat_activity
    where application_name = $1;
    """;

  private static final long STATUS_PRINT_INTERVAL = 500;

  public static void main(String[] args)
  {
    final MainVerticle verticle = new MainVerticle();

    final Vertx vertx = Vertx.vertx();
    logger.info("Deploying main verticle");
    vertx.deployVerticle(verticle, result -> {
      if (result.failed())
      {
        logger.error("Deployment failed. Stopping after 1 second.", result.cause());
        try
        {
          TimeUnit.SECONDS.sleep(1);
        }
        catch (InterruptedException e)
        {
          logger.error("Exit backoff interrupted. Exiting immediately.", e);
        }

        vertx.close(aVoid -> {
          System.out.printf("Vertex close with error: %s%n", result.cause().getMessage());
          System.exit(1);
        });
      }
    });
  }

  private PgPool controlPool;
  private PgPool pool;

  Future<Void> exec(@Language("PostgreSQL") final String query, @NotNull final SqlClient client)
  {
    final Promise<RowSet<Row>> p = Promise.promise();
    client.preparedQuery(query).execute(p);
    return p.future().mapEmpty();
  }

  private final AtomicInteger previousConnectionCount = new AtomicInteger(0);

  void printStatus()
  {
    final Tuple arg = Tuple.of(APPLICATION_NAME);
    final Promise<RowSet<Row>> ret = Promise.promise();
    controlPool.preparedQuery(STATUS_QUERY).execute(arg, ret);
    ret.future()
       .onSuccess(rowSet -> {
         final int newConnectionSize = rowSet.size();
         final int previousConnectionSize = previousConnectionCount.getAndSet(newConnectionSize);
         if (previousConnectionSize != newConnectionSize)
         {
           if (newConnectionSize > MAX_SQL_POOL_SIZE)
           {
             logger.error("\tCONNECTIONS: {}", newConnectionSize);
           }
           else
           {
             logger.info("\tCONNECTIONS: {}", newConnectionSize);
           }
         }
       })
       .onFailure(throwable -> logger.error("Failed to retrieve connection list", throwable))
       .onComplete(ignored -> vertx.setTimer(STATUS_PRINT_INTERVAL, val -> printStatus()));
  }

  private Future<?> dontBlowUp(Transaction tx)
  {
    @Language("PostgreSQL")
    final String badQuery = "INSERT INTO transaction_test (value_a, value_c) VALUES ($1, $2) returning *";
    final Promise<RowSet<Row>> p = Promise.promise();
    tx.preparedQuery(badQuery).execute(Tuple.of(1, "ES"), p);
    return p.future();
  }

  private Future<?> blowUp(Transaction tx)
  {
    @Language("PostgreSQL")
    final String badQuery = "INSERT INTO transaction_test (value_a, value_c) VALUES ($1, $2) returning *";
    final Promise<RowSet<Row>> p = Promise.promise();
    tx.preparedQuery(badQuery).execute(Tuple.of(1, "EOS"), p);
    return p.future();
  }

  <T> Future<?> tx(Function<Transaction, Future<T>> mapper)
  {
    final Promise<Void> ret = Promise.promise();
    final Promise<Transaction> txp = Promise.promise();
    pool.begin(txp);
    return txp.future()
              .compose(tx -> mapper.apply(tx)
                                   .onFailure(throwable -> tx.rollback(asyncResult -> ret.fail(throwable)))
                                   .onSuccess(ignored -> tx.commit(ret)));
  }

  Future<?> dontDoAnythingBad()
  {
    return tx(tx -> {
      final Future<?> b1 = dontBlowUp(tx);
      final Future<?> b2 = dontBlowUp(tx);
      return CompositeFuture.all(b1, b2);
    });
  }

  Future<?> doABadThing()
  {
    return tx(tx -> {
      final Future<?> b1 = blowUp(tx);
      final Future<?> b2 = blowUp(tx);
      return CompositeFuture.all(b1, b2);
    });
  }

  void spawnTransactions()
  {
    final int count = MAX_SQL_POOL_SIZE + 2;
    final List<Future> fs = new ArrayList<>(count);

    for (int i = 0; i < count; i++)
    {
      fs.add(doABadThing());
    }

    CompositeFuture.all(fs).onComplete(ignored -> spawnTransactions());
  }

  void work()
  {
    logger.info("Beginning transaction test.");
    printStatus();
    spawnTransactions();
  }

  PgPool makePool(final String applicationName, final int poolSize)
  {
    final Map<String, String> properties = Map.of("application_name", applicationName);
    final PgConnectOptions connectionOptions = new PgConnectOptions().setPort(PORT)
                                                                     .setDatabase(DATABASE_NAME)
                                                                     .setUser(DATABASE_USER)
                                                                     .setHost(HOST)
                                                                     .setPassword(PASSWORD)
                                                                     .setProperties(properties)
                                                                     .setCachePreparedStatements(
                                                                       CACHE_PREPARED_STATEMENTS);

    final PoolOptions poolOptions = new PoolOptions().setMaxSize(poolSize).setMaxWaitQueueSize(MAX_WAIT_QUEUE_SIZE);
    logger.info("Initializing {} connection pool [{}]", applicationName, poolSize);
    return PgPool.pool(vertx, connectionOptions, poolOptions);
  }

  @Override
  public void start(Promise<Void> startPromise)
  {
    controlPool = makePool("command-line", 1);
    pool = makePool(APPLICATION_NAME, MAX_SQL_POOL_SIZE);

    final Future<Void> createTable = exec("""
                                            CREATE TABLE IF NOT EXISTS transaction_test (
                                                transaction_test_id BIGSERIAL NOT NULL
                                                    CONSTRAINT transaction_test_id_pk
                                                        PRIMARY KEY,
                                                value_a INT,
                                                value_b INT,
                                                value_c varchar(2)
                                            );
                                            """, pool);

    createTable.onSuccess(ignored -> {
      startPromise.complete();
      work();
    }).onFailure(startPromise::fail);
  }
}
