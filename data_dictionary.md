# US Immigration Warehouse Data Dictionary

## imgrtn_data_fct
- arrvl_yr_mnth -- arrival month of immigrant (e.g., 201604)
- ctznshp_cntry_nbr -- country of citizenship of immigrant key (e.g., 999)
- resid_cntry_nbr -- country of resident of immigrant key (e.g., 999)
- port_of_entry_cd -- land, sea or airport port of entry code 
- trans_mode_id -- transportation mode of traveling ID used to entry US
- dest_state_cd -- immigrant destination state code
- visa_ctgry_id -- visa category ID (i.e., 1, 2, 3 etc.)
- airport_id -- airport non-intelligent ID 
- city_demogrphc_id -- non-intelligent city demographics ID 
- city_temp_id -- non-intelligent city temperature ID
- imgrnt_cnt -- number of immigrants associated with combination of keys above

## airport_dim
- airport_id -- non-intelligent airport ID
- airport_native_id -- The text identifier used in the OurAirports URL. This will be the ICAO code if available. Otherwise, it will be a local airport code (if no conflict), or if nothing else is available, an internally-generated code starting with the ISO2 country code, followed by a dash and a four-digit number.
- type_desc  -- The type of the airport. Allowed values are "closed_airport", "heliport", "large_airport", "medium_airport", "seaplane_base", and "small_airport". See the map legend for a definition of each type.
- airport_nm -- The official airport name, including "Airport", "Airstrip", etc. (e.g., (Hartsfield Jackson Atlanta International Airport)
- elevation_ft -- The airport elevation MSL in feet (not metres).
- iso_region_cd -- ISO region code (e.g., US-GA)
- municipality -- The primary municipality that the airport serves (when available). Note that this is not necessarily the municipality where the airport is physically located.
- state_cd -- state code (e.g., GA)
- state_nm -- state name (e.g., Georgia)
- gps_cd -- The code that an aviation GPS database (such as Jeppesen's or Garmin's) would normally use for the airport. This will always be the ICAO code if one exists. Note that, unlike the ident column, this is not guaranteed to be globally unique.
- iata_cd -- The three-letter IATA code for the airport (if it has one).
- local_cd -- The local country code for the airport, if different from the gps_code and iata_code fields (used mainly for US airports).
- longitude -- The airport longitude in decimal degrees (positive for east).
- latitude -- The airport latitude in decimal degrees (positive for north).

## city_demogrphc_dim
- city_demogrphc_id -- non-intelligent city demographics ID
- city_nm -- city name 
- state_cd -- US state/postal code
- state_nm -- US state name 
- median_age -- median age (e.g., 3.5)
- male_pop -- male population
- female_pop -- female population
- total_pop -- total population
- veteran_pop -- veteran population 
- foreign_born_pop -- foreign born population 
- avg_hsehld_sz -- average household size
 
## city_temp_dim
- city_temp_id -- city temperature non-intelligent ID
- yr_mnth -- year and month (e.g., 201604)
- avg_temp -- average temperature 
- avg_temp_uncertainty -- average temperature (Celsius)
- city_nm -- city name 
- cntry_nm -- country name
- latitude -- geographical latitude location, (positive for east) (e.g., 34.56N)
- longitude -- geographical longitude location (positive for north) (e.g., 83.68W)
  
## cntry_dim
- cntry_id -- non-intelligent country ID (e.g., 
- cntry_nm -- country name

## port_of_entry_dim
- port_of_entry_cd -- land, sea or airport port of entry code (e.g., ATL)
- port_full_nm -- port of entry name (e.g., Atlanta, GA)
- city_nm -- city name 
- state_cd -- US state code; postal code (e.g.,GA)
- state_nm -- US state name (e.g., Georgia)
- cntry_cd -- country code (e.g., US)
- cntry_nm -- country name (e.g., United States)
  
## state_dim
- state_cd -- US state postal code (e.g., GA)
- state_nm -- US state name (e.g., Georgia)

## visa_ctgry_dim
- visa_ctgry_id -- visa non-intelligent category ID
- visa_ctgry_nm -- visa category name (e.g., Business, Pleassure, Student, Unknown)

## time_period_dim
- yr_mnth -- year and month (e.g., 201604)
- yr -- year (e.g., 2016)
- mnth -- month (e.g., 4)
- mnth_shrt_nm -- month short name (e.g., Apr)
- mnth_lng_nm -- month long name (e.g., April)
- yr_mnth_nm -- year and month name (e.g., 2016-Apr)
- qtr -- quarter of the year (e.g., 2)
- yr_qtr_nm -- year and quarter name (e.g., 2014-Q2)

## trans_mode_dim
- trans_mode_id -- transportation mode non-intelligent ID
- trans_mode_nm -- transportation mode name (e.g., 
    
