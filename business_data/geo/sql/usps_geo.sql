with d_postal_code AS
    (SELECT postalcode,timezone,dst,type,LatMin,   LongMin,LatMax,LongMax,LatCentroid,LongCentroid,NeighborhoodID, geo_postal_code_row_num
    FROM
        (SELECT  postalcode,timezone,dst,type,LatMin,   LongMin,LatMax,LongMax,LatCentroid,LongCentroid,NeighborhoodID,
         row_number() over(partition by postalcode ORDER BY  etl_create_date desc) AS geo_postal_code_row_num
        FROM geo_xgeo_pdt__geo_postal_code) a
        WHERE geo_postal_code_row_num = 1 ),
     d_neighborhood AS
     (SELECT NeighborhoodID,Neighborhood,CountyID, geo_neighborhood_row_num
    FROM
        (SELECT NeighborhoodID,Neighborhood,CountyID,
        row_number() over(partition by neighborhood ORDER BY  etl_create_date desc) AS geo_neighborhood_row_num
        FROM geo_xgeo_pdt__geo_neighborhood) b
        WHERE geo_neighborhood_row_num = 1 ),
    d_county AS
     (SELECT CountyID, County, FIPS, CountyTypeID, MSAMinorID, geo_county_row_num
    FROM
        (SELECT CountyID, County, FIPS, CountyTypeID, MSAMinorID,
         row_number() over(partition by county
        ORDER BY  etl_create_date desc) AS geo_county_row_num
        FROM geo_xgeo_pdt__geo_county) c WHERE geo_county_row_num = 1 ),
    d_msa_minor as
     (SELECT StateID,MSAMinorID,geo_msaminor_row_num
    FROM
        (SELECT StateID,MSAMinorID,
         row_number() over(partition by msaminor
        ORDER BY  etl_create_date desc) AS geo_msaminor_row_num
        FROM geo_xgeo_pdt__geo_msa_minor) d WHERE geo_msaminor_row_num = 1 ),
    d_state as
     (SELECT stateid, state, fips, SubRegionID,geo_state_row_num
    FROM
        (SELECT stateid, state, fips, SubRegionID, row_number() over(partition by state ORDER BY  etl_create_date desc) AS geo_state_row_num
        FROM geo_xgeo_pdt__geo_state) e
        WHERE geo_state_row_num = 1 ),
    d_subregion as
     (SELECT SubRegionID, RegionID, geo_subregion_row_num
    FROM
        (SELECT SubRegionID, RegionID, row_number() over(partition by subregion ORDER BY  etl_create_date desc) AS geo_subregion_row_num
        FROM geo_xgeo_pdt__geo_subregion) f
        WHERE geo_subregion_row_num = 1 ),
    d_region as
     (SELECT RegionID, CountryID, geo_region_row_num
    FROM
        (SELECT RegionID, CountryID, row_number() over(partition by region
        ORDER BY  etl_create_date desc) AS geo_region_row_num FROM geo_xgeo_pdt__geo_region) g
        WHERE geo_region_row_num = 1 ),
    d_country AS
     (SELECT CountryID, Country, geo_country_row_num
    FROM
        (SELECT CountryID,Country, row_number() over(partition by country
        ORDER BY  etl_create_date desc) AS geo_country_row_num FROM geo_xgeo_pdt__geo_country) h
        WHERE geo_country_row_num = 1 ),
    d_county_type AS
     (SELECT CountyTypeID,CountyType,geo_county_type_row_num
    FROM
        (SELECT CountyTypeID,CountyType,
         row_number() over(partition by countytype
        ORDER BY  etl_create_date desc) AS geo_county_type_row_num
        FROM geo_xgeo_pdt__geo_county_type) c
        WHERE geo_county_type_row_num = 1 ),
    final_table as
    (select
    d_postal_code.postalcode,
    d_postal_code.timezone,
    d_postal_code.dst,
    d_postal_code.type,
    d_postal_code.latMin,
    d_postal_code.longMin,
    d_postal_code.latMax,
    d_postal_code.longMax,
    d_postal_code.latCentroid,
    d_postal_code.longCentroid,
    d_neighborhood.NeighborhoodID,
    d_neighborhood.Neighborhood,
    d_county.CountyID,
    d_county.County,
    d_county.FIPS countyfips,
    d_county.CountyTypeID,
    d_county_type.CountyType,
    d_country.CountryID,
    d_country.Country,
    d_state.stateid,
    d_state.state,
    d_state.fips statefips
    from d_postal_code
    left join d_neighborhood
    on d_postal_code.NeighborhoodID = d_neighborhood.NeighborhoodID
    left join d_county
    on d_neighborhood.CountyID = d_county.CountyID
    left join d_msa_minor
    on d_county.MSAMinorID = d_msa_minor.MSAMinorID
    left join d_state
    on d_msa_minor.StateID = d_state.StateID
    left join d_subregion
    on d_state.SubRegionID = d_subregion.SubRegionID
    left join d_region
    on d_subregion.RegionID = d_region.RegionID
    left join d_country
    on d_region.CountryID = d_country.CountryID
    left join d_county_type
    on d_county.CountyTypeID = d_county_type.CountyTypeID)
    select
    postalcode,
    timezone,
    dst,
    type,
    latMin lat_min,
    longMin long_min,
    latMax lat_max,
    longMax long_max,
    latCentroid lat_centroid,
    longCentroid long_centroid,
    NeighborhoodID neighborhood_id,
    Neighborhood neighborhood,
    CountyID county_id,
    County county,
    countyfips county_fips,
    CountyTypeID county_type_id,
    CountyType county_type,
    CountryID country_id,
    Country country,
    stateid state_id,
    state,
    statefips state_fips
    from final_table
