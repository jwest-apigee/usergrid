package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.UsergridClient;
import org.apache.usergrid.java.client.model.UsergridEntity;
import org.apache.usergrid.java.client.response.UsergridResponse;

import java.util.*;
import java.util.function.Consumer;

/**
 * Created by ApigeeCorporation on 9/3/15.
 */
public class QueryResult
    implements Iterable<UsergridEntity> {

  UsergridClient usergridClient;
  String verb;
  UsergridResponse r;
  UsergridQuery q;
  QueryResult previousQueryResult;
  QueryResult nextQueryResult;
  List<UsergridEntity> entities;
  int entityPointer = 0;
  Map<String, Object> fields;

  public QueryResult(final UsergridClient usergridClient,
                     final String verb,
                     final UsergridResponse r,
                     final UsergridQuery q) {

    this.usergridClient = usergridClient;
    this.verb = verb;
    this.r = r;
    this.q = q;

    if (r != null) {
      this.entities = r.getEntities();
    } else
      this.entities = new ArrayList<>(0);
  }

  public QueryResult(final UsergridClient usergridClient,
                     final String verb,
                     final UsergridResponse r,
                     final UsergridQuery q,
                     final Map<String, Object> fields) {

    this(usergridClient, verb, r, q);
    this.fields = fields;
  }

  public UsergridEntity first() {

    if (entities.size() > 0)
      return r.getEntities().get(0);

    return null;
  }

  public UsergridEntity last() {

    if (entities.size() > 0)
      return r.getEntities().get(r.getEntities().size() - 1);

    return null;
  }

  public boolean hasNext() {
    return entityPointer < this.entities.size();
  }

  public UsergridEntity next() throws NoSuchElementException {
    if (entityPointer < this.entities.size())
      return this.entities.get(entityPointer++);

    throw new NoSuchElementException("No more entities in this page.  Try getting the next page from the QueryResult");
  }

  public List<UsergridEntity> getEntities() {
    return entities;
  }

  public boolean hasMorePages() {
    if (this.r != null) {

      if (this.r.getCursor() != null)
        return true;
    }

    return false;
  }

  public QueryResult retrieveNextPage() {

    if (this.hasMorePages()) {
      String cursor = this.r.getCursor();
      q.getBuilder().cursor(cursor);
      QueryResult next = null;

      switch (this.verb) {
        case "GET":
          next = q.GET();
          next.setPreviousQueryResult(this);
          this.setNextQueryResult(next);
          return next;

        case "PUT":
          //todo PUT?
          next = q.PUT(fields);
          next.setPreviousQueryResult(this);
          this.setNextQueryResult(next);
          return next;

        case "DELETE":
          next = q.DELETE();
          next.setPreviousQueryResult(this);
          this.setNextQueryResult(next);
          return next;
      }
    }

    return new QueryResult(this.usergridClient, this.verb, null, null);
  }

  public void setPreviousQueryResult(QueryResult previousQueryResult) {
    this.previousQueryResult = previousQueryResult;
  }

  public QueryResult getPreviousQueryResult() {
    return previousQueryResult;
  }

  public void setNextQueryResult(QueryResult nextQueryResult) {
    this.nextQueryResult = nextQueryResult;
  }

  public QueryResult getNextQueryResult() {
    if (nextQueryResult == null) {
      nextQueryResult = retrieveNextPage();
    }

    return nextQueryResult;
  }

  @Override
  public Iterator<UsergridEntity> iterator() {
    return entities.iterator();
  }

  @Override
  public void forEach(Consumer<? super UsergridEntity> action) {
    Objects.requireNonNull(action);

    for (UsergridEntity t : this) {
      action.accept(t);
    }
  }

  @Override
  public Spliterator<UsergridEntity> spliterator() {
    return entities.spliterator();
  }
}
