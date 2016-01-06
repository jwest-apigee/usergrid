package org.apache.usergrid.java.client.query;

import javax.annotation.Nullable;
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

  private UsergridClient usergridClient;
  private String verb;
  private UsergridResponse r;
  private String cursor;
  private UsergridQuery q;
  private QueryResult previousQueryResult;
  private QueryResult nextQueryResult;
  private List<UsergridEntity> entities;
  private int entityPointer = 0;
  private Map<String, Object> fields;
  private boolean cleared = false;

  public QueryResult(final UsergridClient usergridClient,
                     final String verb,
                     final UsergridResponse r,
                     final UsergridQuery q) {

    this.usergridClient = usergridClient;
    this.verb = verb;
    this.r = r;
    this.q = q;
    this.cleared = false;

    if (r != null) {
      this.cursor = r.getCursor();
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

  @Nullable
  public UsergridEntity first() {

    if (entities.size() > 0)
      return r.getEntities().get(0);

    return null;
  }

  @Nullable
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
    // if not cleared then return entities
    // otherwise make an API call
    if (cleared == false) {
      q.getBuilder().cursor(cursor);
      QueryResult next = q.GET();

      this.r = next.r;
    }

    return entities;
  }

  public boolean hasMorePages() {
    return this.cursor != null;
  }

  public QueryResult retrieveNextPage() {

    if (this.hasMorePages()) {
      String cursor = this.cursor;
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

  private QueryResult findRoot() {
    QueryResult temp = this;

    // find root
    while (temp.previousQueryResult != null)
      temp = temp.previousQueryResult;

    return temp;
  }

  public void clear() {
    this.r = null;
    this.entities.clear();
    this.cleared = true;
  }

  public void clearList() {
    findRoot().destroy();
  }

  public QueryResult getPage(int number) {

    QueryResult qr = findRoot();

    int x = 0;

    while (x <= number) {

      if (qr.nextQueryResult != null)
        qr = qr.nextQueryResult;

      else
        // throw not found
        break;

      x++;
    }

    return qr;
  }

  private void destroy() {

    if (this.nextQueryResult != null)
      this.nextQueryResult.destroy();

    this.r = null;
    this.q = null;
    this.entities.clear();

    this.nextQueryResult = null;
  }
}
