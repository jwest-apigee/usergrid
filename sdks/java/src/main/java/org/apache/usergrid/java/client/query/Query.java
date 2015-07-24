package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.Client;
import org.apache.usergrid.java.client.Usergrid;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ApigeeCorporation on 7/1/15.
 */

public class Query {

  public static final String ORDER_BY = " ORDER BY ";
  public static final String LIMIT = " LIMIT ";
  public static final String AND = " AND ";
  public static final String EQUALS = "=";
  public static final String AMPERSAND = "&";
  public static final String SPACE = " ";
  public static final String ASTERISK = "*";
  private final QueryBuilder queryBuilder;

  public static void main(String[] args) {
    Query q = new QueryBuilder()
        .collection("pets")
        .limit(100)
        .gt("age", 100)
        .gte("age", 100)
        .containsWord("field", "value")
        .desc("cats")
        .asc("dogs")
        .build();

    System.out.println(q.build());
  }

  public Query(QueryBuilder queryBuilder) {
    this.queryBuilder = queryBuilder;
  }

  public String build() {
    String urlAppend = "";
    boolean hasContent = false;

    if (this.queryBuilder.requirements.size() > 0) {
      String qlString = "";

      for (int i = 0; i < this.queryBuilder.requirements.size(); i++) {

        if (i > 0) {
          qlString += AND;
        }

        qlString += this.queryBuilder.requirements.get(i);
      }

      if (this.queryBuilder.orderClauses != null && this.queryBuilder.orderClauses.size() > 0) {
        for (int i = 0; i < this.queryBuilder.orderClauses.size(); i++) {

          if (i == 0) {
            qlString += ORDER_BY;
          }
          SortTerm term = this.queryBuilder.orderClauses.get(i);

          qlString += term.term + SPACE + term.order;

          if (i < this.queryBuilder.orderClauses.size() - 1) {
            qlString += COMMA;
          }
        }
      }


//      qlString = QueryResult.encode(qlString);
      urlAppend = QL + EQUALS + qlString;
      hasContent = true;
    }

    if (this.queryBuilder.urlTerms.size() > 0) {

      for (String urlTerm : this.queryBuilder.urlTerms) {

        if (hasContent) {
          urlAppend += AMPERSAND + urlTerm;
        }

        hasContent = true;
      }
    }

    if (this.queryBuilder.limit != Integer.MIN_VALUE) {
    }

    return urlAppend;
  }

  private static String encode(final String stringValue) {
    String escapedString;
    try {
      escapedString = URLEncoder.encode(stringValue, UTF8);
    } catch (Exception e) {
      escapedString = stringValue;
    }
    return escapedString;
  }

  public Usergrid.QueryResult get() {
    return Usergrid.getInstance().query(this);
  }

  public static class QueryBuilder {

    public final ArrayList<String> requirements = new ArrayList<String>();
    public final ArrayList<String> urlTerms = new ArrayList<String>();
    public String collectionName;
    public int limit = Integer.MIN_VALUE;
    public List<SortTerm> orderClauses;

    public QueryBuilder() {
    }

    private void addRequirement(final String requirement) {
      if (requirement != null) {
        this.requirements.add(requirement);
      }
    }

    private QueryBuilder addOperationRequirement(final String term, final QUERY_OPERATION operation, final String value) {
      if (term != null && operation != null && value != null) {
        this.addRequirement(term + operation.toString() + value);
      }
      return this;
    }

    private QueryBuilder addOperationRequirement(final String term, final QUERY_OPERATION operation, final int value) {
      if (term != null && operation != null) {
        addRequirement(term + operation.toString() + value);
      }
      return this;
    }

    public QueryBuilder startsWith(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + EQUALS + APOSTROPHE + value + ASTERISK + APOSTROPHE);
      }
      return this;
    }

    public QueryBuilder endsWith(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + EQUALS + APOSTROPHE + ASTERISK + value + APOSTROPHE);
      }
      return this;
    }

    public QueryBuilder containsString(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + CONTAINS + APOSTROPHE + value + APOSTROPHE);
      }
      return this;
    }

    public QueryBuilder containsWord(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + CONTAINS + APOSTROPHE + value + APOSTROPHE);
      }
      return this;
    }

    public QueryBuilder in(final String term, final int low, final int high) {
      if (term != null) {
        addRequirement(term + IN + low + COMMA + high);
      }
      return this;
    }

    public QueryBuilder locationWithin(final String term, final float distance, final float latitude, final float longitude) {
      if (term != null) {
        addRequirement(term + WITHIN + distance + OF + latitude + COMMA + longitude);
      }
      return this;
    }

    public QueryBuilder collection(final String collectionName) {
      if (collectionName != null) {
        this.collectionName = collectionName;
      }
      return this;
    }

    public QueryBuilder urlTerm(final String urlTerm, final String equalsValue) {
      if (urlTerm != null && equalsValue != null) {
        if (urlTerm.equalsIgnoreCase(QL)) {
          ql(equalsValue);
        } else {
          urlTerms.add(Query.encode(urlTerm) + "=" + Query.encode(equalsValue));
        }
      }
      return this;
    }

    public QueryBuilder ql(final String value) {
      if (value != null) {
        addRequirement(value);
      }
      return this;
    }

    public QueryBuilder equals(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.EQUAL, stringValue);
    }

    public QueryBuilder equals(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.EQUAL, intValue);
    }

    public QueryBuilder greaterThan(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN, stringValue);
    }

    public QueryBuilder greaterThan(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN, intValue);
    }

    public QueryBuilder greaterThanOrEqual(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN_EQUAL_TO, stringValue);
    }

    public QueryBuilder greaterThanOrEqual(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN_EQUAL_TO, intValue);
    }

    public QueryBuilder lessThan(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN, stringValue);
    }

    public QueryBuilder lessThan(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN, intValue);
    }

    public QueryBuilder lessThanOrEqual(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN_EQUAL_TO, stringValue);
    }

    public QueryBuilder lessThanOrEqual(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN_EQUAL_TO, intValue);
    }

    public QueryBuilder filter(final String term, final String stringValue) {
      return this.equals(term, stringValue);
    }

    public QueryBuilder filter(final String term, final int intValue) {
      return this.equals(term, intValue);
    }

    public QueryBuilder eq(final String term, final String stringValue) {
      return this.equals(term, stringValue);
    }

    public QueryBuilder eq(final String term, final int intValue) {
      return this.equals(term, intValue);
    }

    public QueryBuilder gt(final String term, final String stringValue) {
      return this.greaterThan(term, stringValue);
    }

    public QueryBuilder gt(final String term, final int intValue) {
      return this.greaterThan(term, intValue);
    }

    public QueryBuilder gte(final String term, final String stringValue) {
      return this.greaterThanOrEqual(term, stringValue);
    }

    public QueryBuilder gte(final String term, final int intValue) {
      return this.greaterThanOrEqual(term, intValue);
    }

    public QueryBuilder lt(final String term, final String stringValue) {
      return this.lessThan(term, stringValue);
    }

    public QueryBuilder lt(final String term, final int intValue) {
      return this.lessThan(term, intValue);
    }

    public QueryBuilder lte(final String term, final String stringValue) {
      return this.lessThanOrEqual(term, stringValue);
    }

    public QueryBuilder lte(final String term, final int intValue) {
      return this.lessThanOrEqual(term, intValue);
    }

    public QueryBuilder asc(String term) {
      return this.descending(term);
    }

    public QueryBuilder ascending(String term) {
      return addSortTerm(new SortTerm(term, "ASC"));
    }

    public QueryBuilder desc(String term) {
      return this.descending(term);
    }

    public QueryBuilder descending(String term) {
      return addSortTerm(new SortTerm(term, "DESC"));
    }

    private QueryBuilder addSortTerm(SortTerm term) {

      if (orderClauses == null) {
        orderClauses = new ArrayList<SortTerm>(3);
      }

      orderClauses.add(term);

      return this;
    }

    public Query build() {
      return new Query(this);
    }

    public QueryBuilder limit(int limit) {
      this.limit = limit;
      return this;
    }
  }

  private enum QUERY_OPERATION {
    EQUAL(" = "), GREATER_THAN(" > "), GREATER_THAN_EQUAL_TO(" >= "), LESS_THAN(" < "), LESS_THAN_EQUAL_TO(" <= ");
    private final String stringValue;

    QUERY_OPERATION(final String s) {
      this.stringValue = s;
    }

    public String toString() {
      return this.stringValue;
    }
  }

  private static final String APOSTROPHE = "'";
  private static final String COMMA = ",";
  private static final String CONTAINS = " contains ";
  private static final String IN = " in ";
  private static final String OF = " of ";
  private static final String QL = "ql";
  private static final String UTF8 = "UTF-8";
  private static final String WITHIN = " within ";
}
