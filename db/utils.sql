-- Create Tables --
CREATE TABLE bike_station_status("id" integer PRIMARY KEY,  "full" smallint, "empty" smallint, "updated_at" timestamp (0));
CREATE TABLE weather_zone ("id" int PRIMARY KEY, "name" char(20), "latitude" real, "longitude" real);
CREATE TABLE bike_station ("id" int PRIMARY KEY, "latitude" real, "longitude" real, "address" char(30), "city" char(20), "name" char(20), "area" char(20), "weather_zone_id" int, "created_at" timestamp);
CREATE TABLE fill_rate_forecast("id" serial PRIMARY KEY, "station_id" int, "fill_rate" real, "relative_ts" smallint, "base_ts" timestamp, "run_ts" timestamp);

