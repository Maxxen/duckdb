#include <geos/geom/Geometry.h>
#include <geos/geom/GeometryFactory.h>

#include "geo_common.hpp"
#include "geo_types.hpp"

namespace duckdb  {

namespace geo {

static inline std::unique_ptr<geos::geom::Point> PointToGEOS(double x, double y, int srid = 0) {

    auto factory = geos::geom::GeometryFactory::create();
    std::unique_ptr<geos::geom::Point> geom(factory->createPoint(geos::geom::Coordinate(x, y)));
    geom->setSRID(srid);
    
    return geom;
}

} // namespace geo

} // namespace duckdb