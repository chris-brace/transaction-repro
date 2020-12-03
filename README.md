Attempt to demonstrate that the vertx-pg client fails to
propperly release rolled back transactions and will, under
certain circumstances, ignore the pool's max connection setting.
