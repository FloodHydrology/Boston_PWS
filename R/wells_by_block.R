#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Title: Wells by census block
#Date: 8/14/2022
#Coder: Nate Jones (cnjones7@ua.edu)
#Subject: Estimate number of wells per township
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 1: Setup workspace -------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Clear memory
remove(list=ls())

#load libraries of interest
library(tidyverse)
library(raster)
library(sf)
library(parallel)

#load data
towns  <- st_read('data/gb.shp')
blocks <- st_read('data/tl_2010_25_tabblock10.shp')
wells  <- raster('data/REM_map_2010.tif')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 2: prep data -------------------------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Define common projection
p <- wells@crs

#Reproject layers of interest
towns  <- towns %>% st_transform(., crs=st_crs(p)) %>% st_zm()
blocks <- blocks %>% st_transform(., crs=st_crs(p)) %>% st_zm()

#Define common extent
m <- towns %>% extent(.) %>% as(., 'SpatialPolygons') %>% st_as_sf(.)
st_crs(m) <- p

#Crop data to greater boston area
blocks <- st_intersection(blocks, m)
wells  <- wells %>% crop(., m) %>% mask(., m) 

#Create raster of higher resolution
wells<-raster::disaggregate(wells, fact = 100)/(100^2)

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 3: Create extractorize function ------------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#From github gists: https://gist.github.com/FloodHydrology/669f1d704555f98abacfdb37c41e7794
#Function
extracterize<-function(r, #Large Raster
                       p #Inidividual Polygon 
){
  #load libraries of interest
  library(tidyverse)
  library(raster)
  library(sf)
  
  #Crop raster to polygon (w/100 m buffer)
  r <- r %>% crop(., st_buffer(p, 100)) 
  
  #convert raster to points
  r <- rasterToPoints(r, spatial = T) %>% st_as_sf
  r <- st_intersection(r, p)

  #Create output
  tibble(
    GEOID10 = p$GEOID10,
    well_pop = sum(r$REM_map_2010, na.rm = T)
  )
}

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Step 4: Extract raster data by township ---------------------------------------
#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
#Apply function to blocks 
#Create wrapper function
fun <- function(n){
  extracterize(
    r = wells,
    p = blocks[n,])}

#Create error catching wrapper function
error_fun<-function(x){
  tryCatch(
    expr  = fun(x), 
    error = function(e){
              tibble(
                uid = blocks$GEOID10[x],
                value = NA)}
  )
}

#prepare cores
n.cores<-detectCores()-1
cl<-makeCluster(n.cores)
clusterEvalQ(cl, {
  library(tidyverse)
  library(sf)
  library(raster)
})
clusterExport(cl, c("fun", "extracterize" ,"wells", "blocks"))
  
#Now run function
output<-parLapply(cl = cl, seq(1, nrow(blocks)), error_fun) %>% bind_rows()
output

#Turn off clusters
stopCluster(cl)

#Export
blocks_export<-left_join
st_write(blocks, "docs/blocks_well_pop.shp")

#
# write.csv(export, "export.csv")
