package org.apache.usergrid.java.client.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import java.io.Serializable;

/**
 * Created by Jeff West @ ApigeeCorporation on 9/3/15.
 */
public class Location implements Serializable{
  float latitude, longitude;

  public Location(float latitude, float longitude) {
    this.latitude = latitude;
    this.longitude = longitude;
  }

  @JsonSerialize
  public float getLatitude() {
    return latitude;
  }

  @JsonProperty("latitude")
  public void setLatitude(float latitude) {
    this.latitude = latitude;
  }

  @JsonSerialize
  public float getLongitude() {
    return longitude;
  }

  @JsonProperty("longitude")
  public void setLongitude(float longitude) {
    this.longitude = longitude;
  }
}
