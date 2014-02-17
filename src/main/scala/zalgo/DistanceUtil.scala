package zalgo

import math._

// Code courtesy of Textgrounder: https://bitbucket.org/utcompling/textgrounder

object DistanceUtil {

  /** *** Fixed values *****/

  val minimum_latitude = -90.0
  val maximum_latitude = 90.0
  val minimum_longitude = -180.0
  val maximum_longitude = 180.0 - 1e-10

  // Radius of the earth in miles.  Used to compute spherical distance in miles,
  // and miles per degree of latitude/longitude.
  val earth_radius_in_miles = 3963.191
  val meter_per_mile = 1609.34

  // Number of kilometers per mile.
  val km_per_mile = 1.609

  // Number of miles per degree, at the equator.  For longitude, this is the
  // same everywhere, but for latitude it is proportional to the degrees away
  // from the equator.
  val miles_per_degree = Pi * 2 * earth_radius_in_miles / 360.0

  /** *** Computed values based on command-line params *****/

  // Size of each region in degrees.  Determined by the --degrees-per-region
  // option, unless --miles-per-region is set, in which case it takes
  // priority.
  var degrees_per_region = 0.0

  // Size of each region (vertical dimension; horizontal dimension only near
  // the equator) in miles.  Determined from degrees_per_region.
  var miles_per_region = 0.0

  var width_of_stat_region = 1

  // A 2-dimensional coordinate.
  //
  // The following fields are defined:
  //
  //   lat, long: Latitude and longitude of coordinate.
  case class Coord(lat: Double, long: Double,
                   validate: Boolean = true) {
    if (validate) {
      // Not sure why this code was implemented with coerce_within_bounds,
      // but either always coerce, or check the bounds ...
      require(lat >= minimum_latitude)
      require(lat <= maximum_latitude)
      require(long >= minimum_longitude)
      require(long <= maximum_longitude)
    }

    override def toString() = "(%.2f,%.2f)".format(lat, long)
  }

  object Coord {
    // Create a coord, with METHOD defining how to handle coordinates
    // out of bounds.  If METHOD = "accept", just accept them; if
    // "validate", check within bounds, and abort if not.  If "coerce",
    // coerce within bounds (latitudes are cropped, longitudes are taken
    // mod 360).
    def apply(lat: Double, long: Double, method: String) = {
      var validate = false
      val (newlat, newlong) =
        method match {
          case "coerce-warn" => {
            coerce(lat, long)
          }
          case "coerce" => coerce(lat, long)
          case "validate" => {
            validate = true; (lat, long)
          }
          case "accept" => {
            (lat, long)
          }
          case _ => {
            require(false,
              "Invalid method to Coord(): %s" format method)
            (0.0, 0.0)
          }
        }
      new Coord(newlat, newlong, validate = validate)
    }

    def valid(lat: Double, long: Double) =
      lat >= minimum_latitude &&
      lat <= maximum_latitude &&
      long >= minimum_longitude &&
      long <= maximum_longitude

    def coerce(lat: Double, long: Double) = {
      var newlat = lat
      var newlong = long
      if (newlat > maximum_latitude) newlat = maximum_latitude
      while (newlong > maximum_longitude) newlong -= 360.0
      if (newlat < minimum_latitude) newlat = minimum_latitude
      while (newlong < minimum_longitude) newlong += 360.0
      (newlat, newlong)
    }
  }

  // Compute spherical distance in METERS (along a great circle) between two
  // coordinates.

  def spheredist(p1: Coord, p2: Coord): Double = {
    if (p1 == null || p2 == null) return 1000000.0
    val thisRadLat = (p1.lat / 180.0) * Pi
    val thisRadLong = (p1.long / 180.0) * Pi
    val otherRadLat = (p2.lat / 180.0) * Pi
    val otherRadLong = (p2.long / 180.0) * Pi

    val angle_cos = (sin(thisRadLat) * sin(otherRadLat)
      + cos(thisRadLat) * cos(otherRadLat) *
      cos(otherRadLong - thisRadLong))
    // If the values are extremely close to each other, the resulting cosine
    // value will be extremely close to 1.  In reality, however, if the values
    // are too close (e.g. the same), the computed cosine will be slightly
    // above 1, and acos() will complain.  So special-case this.
    if (abs(angle_cos) > 1.0) {
      if (abs(angle_cos) > 1.000001) {
        return 1000000.0
      } else
        return 0.0
    }

    earth_radius_in_miles * meter_per_mile * acos(angle_cos)
  }
}
