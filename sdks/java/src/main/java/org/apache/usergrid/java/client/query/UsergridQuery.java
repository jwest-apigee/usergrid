package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.response.ApiResponse;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by ApigeeCorporation on 7/1/15.
 */

public class UsergridQuery {

  public static final String ORDER_BY = " ORDER BY ";
  public static final String LIMIT = " LIMIT ";
  public static final String AND = " AND ";
  public static final String EQUALS = "=";
  public static final String AMPERSAND = "&";
  public static final String SPACE = " ";
  public static final String ASTERISK = "*";
  private final Builder builder;

  public static void main(String[] args) {
    UsergridQuery q = new Builder()
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

  public UsergridQuery(Builder builder) {
    this.builder = builder;
  }

  public String build() {
    String urlAppend = "";
    boolean hasContent = false;

    if (this.builder.requirements.size() > 0) {
      String qlString = "";

      for (int i = 0; i < this.builder.requirements.size(); i++) {

        if (i > 0) {
          qlString += AND;
        }

        qlString += this.builder.requirements.get(i);
      }

      if (this.builder.orderClauses != null && this.builder.orderClauses.size() > 0) {
        for (int i = 0; i < this.builder.orderClauses.size(); i++) {

          if (i == 0) {
            qlString += ORDER_BY;
          }
          SortTerm term = this.builder.orderClauses.get(i);

          qlString += term.term + SPACE + term.order;

          if (i < this.builder.orderClauses.size() - 1) {
            qlString += COMMA;
          }
        }
      }


//      qlString = QueryResult.encode(qlString);
      urlAppend = QL + EQUALS + qlString;
      hasContent = true;
    }

    if (this.builder.urlTerms.size() > 0) {

      for (String urlTerm : this.builder.urlTerms) {

        if (hasContent) {
          urlAppend += AMPERSAND + urlTerm;
        }

        hasContent = true;
      }
    }

    //todo finish

    if (this.builder.limit != Integer.MIN_VALUE) {
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

  public QueryResult get() {
    return Usergrid.getInstance().get(this);
  }

  public ApiResponse put(Map<String, Object> fields) {
    return Usergrid.getInstance().put(this, fields);
  }

  public static class Builder {

    public final ArrayList<String> requirements = new ArrayList<String>();
    public final ArrayList<String> urlTerms = new ArrayList<String>();
    public String collectionName;
    public int limit = Integer.MIN_VALUE;
    public List<SortTerm> orderClauses;

    public Builder() {
    }

    private void addRequirement(final String requirement) {
      if (requirement != null) {
        this.requirements.add(requirement);
      }
    }

    private Builder addOperationRequirement(final String term, final QUERY_OPERATION operation, final String value) {
      if (term != null && operation != null && value != null) {
        this.addRequirement(term + operation.toString() + value);
      }
      return this;
    }

    private Builder addOperationRequirement(final String term, final QUERY_OPERATION operation, final int value) {
      if (term != null && operation != null) {
        addRequirement(term + operation.toString() + value);
      }
      return this;
    }

    public Builder startsWith(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + EQUALS + APOSTROPHE + value + ASTERISK + APOSTROPHE);
      }
      return this;
    }

    public Builder endsWith(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + EQUALS + APOSTROPHE + ASTERISK + value + APOSTROPHE);
      }
      return this;
    }

    public Builder containsString(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + CONTAINS + APOSTROPHE + value + APOSTROPHE);
      }
      return this;
    }

    public Builder containsWord(final String term, final String value) {
      if (term != null && value != null) {
        addRequirement(term + CONTAINS + APOSTROPHE + value + APOSTROPHE);
      }
      return this;
    }

    public Builder in(final String term, final int low, final int high) {
      if (term != null) {
        addRequirement(term + IN + low + COMMA + high);
      }
      return this;
    }

    public Builder locationWithin(final String term, final float distance, final float latitude, final float longitude) {
      if (term != null) {
        addRequirement(term + WITHIN + distance + OF + latitude + COMMA + longitude);
      }
      return this;
    }

    public Builder collection(final String collectionName) {
      if (collectionName != null) {
        this.collectionName = collectionName;
      }
      return this;
    }

    public Builder urlTerm(final String urlTerm, final String equalsValue) {
      if (urlTerm != null && equalsValue != null) {
        if (urlTerm.equalsIgnoreCase(QL)) {
          ql(equalsValue);
        } else {
          urlTerms.add(UsergridQuery.encode(urlTerm) + "=" + UsergridQuery.encode(equalsValue));
        }
      }
      return this;
    }

    public Builder ql(final String value) {
      if (value != null) {
        addRequirement(value);
      }
      return this;
    }

    public Builder equals(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.EQUAL, stringValue);
    }

    public Builder equals(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.EQUAL, intValue);
    }

    public Builder greaterThan(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN, stringValue);
    }

    public Builder greaterThan(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN, intValue);
    }

    public Builder greaterThanOrEqual(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN_EQUAL_TO, stringValue);
    }

    public Builder greaterThanOrEqual(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.GREATER_THAN_EQUAL_TO, intValue);
    }

    public Builder lessThan(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN, stringValue);
    }

    public Builder lessThan(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN, intValue);
    }

    public Builder lessThanOrEqual(final String term, final String stringValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN_EQUAL_TO, stringValue);
    }

    public Builder lessThanOrEqual(final String term, final int intValue) {
      return addOperationRequirement(term, QUERY_OPERATION.LESS_THAN_EQUAL_TO, intValue);
    }

    public Builder filter(final String term, final String stringValue) {
      return this.equals(term, stringValue);
    }

    public Builder filter(final String term, final int intValue) {
      return this.equals(term, intValue);
    }

    public Builder eq(final String term, final String stringValue) {
      return this.equals(term, stringValue);
    }

    public Builder eq(final String term, final int intValue) {
      return this.equals(term, intValue);
    }

    public Builder gt(final String term, final String stringValue) {
      return this.greaterThan(term, stringValue);
    }

    public Builder gt(final String term, final int intValue) {
      return this.greaterThan(term, intValue);
    }

    public Builder gte(final String term, final String stringValue) {
      return this.greaterThanOrEqual(term, stringValue);
    }

    public Builder gte(final String term, final int intValue) {
      return this.greaterThanOrEqual(term, intValue);
    }

    public Builder lt(final String term, final String stringValue) {
      return this.lessThan(term, stringValue);
    }

    public Builder lt(final String term, final int intValue) {
      return this.lessThan(term, intValue);
    }

    public Builder lte(final String term, final String stringValue) {
      return this.lessThanOrEqual(term, stringValue);
    }

    public Builder lte(final String term, final int intValue) {
      return this.lessThanOrEqual(term, intValue);
    }

    public Builder asc(String term) {
      return this.descending(term);
    }

    public Builder ascending(String term) {
      return addSortTerm(new SortTerm(term, "ASC"));
    }

    public Builder desc(String term) {
      return this.descending(term);
    }

    public Builder descending(String term) {
      return addSortTerm(new SortTerm(term, "DESC"));
    }

    private Builder addSortTerm(SortTerm term) {

      if (orderClauses == null) {
        orderClauses = new ArrayList<SortTerm>(3);
      }

      orderClauses.add(term);

      return this;
    }

    public UsergridQuery build() {
      return new UsergridQuery(this);
    }

    public Builder limit(int limit) {
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
