package org.apache.usergrid.java.client.query;

import org.apache.usergrid.java.client.Usergrid;
import org.apache.usergrid.java.client.response.UsergridResponse;

import java.net.URLEncoder;
import java.util.*;

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
  private static final String APOSTROPHE = "'";
  private static final String COMMA = ",";
  private static final String CONTAINS = " contains ";
  private static final String IN = " in ";
  private static final String OF = " of ";
  private static final String QL = "ql";
  private static final String UTF8 = "UTF-8";
  private static final String WITHIN = " within ";

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

  public Builder getBuilder() {
    return builder;
  }

  public String build() {

    String selectString = this.builder.getSelectString();

    String predicateString = this.builder.getPredicateString();

    String urlAppend = "?" + QL + "=" + selectString + " " + predicateString;

    urlAppend += AMPERSAND + this.builder.getQueryParameterString();

    return this.builder.collectionName + urlAppend;
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

  public QueryResult GET() {
    return Usergrid.getInstance().GET(this);
  }

  public QueryResult DELETE() {
    return Usergrid.getInstance().DELETE(this);
  }

  public QueryResult PUT(final Map<String, Object> stringObjectHashMap) {
    return Usergrid.getInstance().PUT(this, stringObjectHashMap);
  }


  public HashMap<String, Object> params() {
    HashMap<String, Object> items = new HashMap<>();

    items.putAll(builder.urlTerms);
    items.put("ql", builder.getSelectString() + builder.getPredicateString());

    return items;
  }

  public String getCollectionName() {
    return builder.collectionName;
  }


  public static class Builder {

    public final ArrayList<String> requirements = new ArrayList<>();
    public final Map<String, String> urlTerms = new HashMap<>(11);
    public String collectionName;
    public List<SortTerm> orderClauses;
    public Set<String> selectFields;

    public Builder() {
    }

    private void addRequirement(final String requirement) {
      if (requirement != null) {
        this.requirements.add(requirement);
      }
    }

    private Builder addOperationRequirement(final String term, final QUERY_OPERATION operation, final String value) {

      if (term != null && operation != null && value != null) {
        this.addRequirement(term + operation.toString() + APOSTROPHE + value + APOSTROPHE);
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

    public Builder locationWithin(final float distance, final float latitude, final float longitude) {

      addRequirement("location " + WITHIN + distance + OF + latitude + COMMA + longitude);

      return this;
    }

    public Builder locationWithin(final double distance, final double latitude, final double longitude) {

      addRequirement("location " + WITHIN + distance + OF + latitude + COMMA + longitude);

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
          urlTerms.put(UsergridQuery.encode(urlTerm), UsergridQuery.encode(equalsValue));
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

      this.urlTerm("limit", String.valueOf(limit));
      return this;
    }

    public Builder cursor(String cursor) {
      this.urlTerm("cursor", cursor);
      return this;
    }

    public Builder field(String field) {
      if (selectFields == null) {
        selectFields = new HashSet<>(3);
      }

      selectFields.add(field);

      return this;
    }

    public String getSelectString() {
      if (this.selectFields == null || this.selectFields.size() == 0)
        return "select * ";

      String selectString = "select ";
      String appendString = ", ";

      for (String field : this.selectFields)
        selectString += field + appendString;

      return selectString.substring(0, selectString.length() - appendString.length());
    }

    public String getQueryParameterString() {
      String qparamString = "";

      if (this.urlTerms.size() > 0) {

        for (Map.Entry<String, String> urlTermPair : this.urlTerms.entrySet()) {

          String urlTerm = urlTermPair.getKey();
          String equalsValue = urlTermPair.getValue();
          if (qparamString.length() > 0)
            qparamString += AMPERSAND;

          qparamString += UsergridQuery.encode(urlTerm) + "=" + UsergridQuery.encode(equalsValue);
        }

      }
      return qparamString;
    }

    public String getPredicateString() {

      String predicatesString = "";

      for (int i = 0; i < this.requirements.size(); i++) {

        if (i > 0) {
          predicatesString += AND;
        }

        predicatesString += this.requirements.get(i);
      }

      String qlString = "";

      if (this.requirements.size() > 0)
        qlString = " WHERE " + predicatesString;

      if (this.orderClauses != null && this.orderClauses.size() > 0) {
        for (int i = 0; i < this.orderClauses.size(); i++) {

          if (i == 0) {
            qlString += ORDER_BY;
          }
          SortTerm term = this.orderClauses.get(i);

          qlString += term.term + SPACE + term.order;

          if (i < this.orderClauses.size() - 1) {
            qlString += COMMA;
          }
        }
      }

      return qlString;
    }
  }

  private enum QUERY_OPERATION {
    EQUAL(" = "),
    GREATER_THAN(" > "),
    GREATER_THAN_EQUAL_TO(" >= "),
    LESS_THAN(" < "),
    LESS_THAN_EQUAL_TO(" <= ");

    private final String stringValue;

    QUERY_OPERATION(final String s) {
      this.stringValue = s;
    }

    public String toString() {
      return this.stringValue;
    }
  }
}


