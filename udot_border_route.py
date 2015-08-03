__author__ = 'wzhang'

import arcpy
import os
import sys
import math
import logging
import logging.config
import logging.handlers
from datetime import datetime
import traceback

logger = logging.getLogger(__name__)

def main():
    setup_logger()

    # input parameters
    config = get_parameters()
    section = "Default"

    workspace = config.get(section, "WORKSPACE")
    route = config.get(section, "ROUTE")
    route_id_field = config.get(section,"ROUTE_ID_FIELD")
    boundaries = config.get(section, "BOUNDARY")
    boundaries_id_fields = config.get(section, "BOUNDARY_ID_FIELD")
    route_border_rule_tables = config.get(section, "ROUTE_BORDER_RULE_TABLE")
    buffer_size  = config.get(section, "BUFFER_SIZE")
    high_angle_threshold  = float(config.get(section, "HIGH_ANGLE_THRESHOLD"))
    offset  = config.get(section, "OFFSET")

    boundaries = boundaries.split(",")
    boundaries_id_fields = boundaries_id_fields.split(",")
    route_border_rule_tables = route_border_rule_tables.split(",")

    arcpy.env.workspace = workspace
    arcpy.env.overwriteOutput = True

    # generate route border rule source tables
    boundary = boundaries[0]
    boundary_id_field = boundaries_id_fields[0]
    route_border_rule_table = route_border_rule_tables[0]

    arcpy.AddMessage("Working on {0} and {1}".format(route,boundary))
    generate_route_border_rule_table(workspace,route,route_id_field,boundary,boundary_id_field,buffer_size,route_border_rule_table,high_angle_threshold,offset)

    # for boundary in boundaries:
    #     index = boundaries.index(boundary)
    #     boundary_id_field = boundaries_id_fields[index]
    #     route_border_rule_table = route_border_rule_tables[index]
    #
    #
    #     if not generate_route_border_rule_table(workspace,route,route_id_field,boundary,boundary_id_field,buffer_size,route_border_rule_table,high_angle_threshold,offset):
    #         sys.exit("Failed when generating border route rule source table for {0} feature. Exit")


def generate_route_border_rule_table(workspace,route,route_id_field,boundary,boundary_id_field,buffer_size,route_border_rule_table,high_angle_threshold,offset):
    arcpy.AddMessage("Generating route border rule source table for {0}...".format(boundary))
    try:
        date = datetime.now()
        date_string = date.strftime("%m/%d/%Y")

        spatial_reference = arcpy.Describe(route).spatialReference
        xy_resolution = "{0} {1}".format(spatial_reference.XYResolution,spatial_reference.linearUnitName)

        ###############################################################################################################
        # get all candidate border routes by boundary buffer
        arcpy.AddMessage("Identifying candidate border routes...")

        # generate boundary border
        boundary_border = os.path.join(workspace,"{0}_boundary_border".format(boundary))
        arcpy.FeatureToLine_management(boundary, boundary_border)

        # dissolve polygon boundary based on boundary id
        boundary_border_dissolved = os.path.join(workspace,"{0}_boundary_border_dissolved".format(boundary))
        arcpy.Dissolve_management(boundary_border,boundary_border_dissolved,[boundary_id_field])

        # generate buffer around boundary
        # arcpy.AddMessage("generate buffer around boundary")
        boundary_border_buffer = os.path.join(workspace,"{0}_boundary_buffer".format(boundary))
        arcpy.Buffer_analysis(boundary_border_dissolved, boundary_border_buffer, buffer_size, "FULL", "ROUND")

        # get candidate border route
        # arcpy.AddMessage("get candidate border route")
        candidate_border_route_multipart = "in_memory\\candidate_{0}_border_route_multipart".format(boundary)
        candidate_border_route = os.path.join(workspace,"candidate_{0}_border_route".format(boundary))
        arcpy.Clip_analysis(route, boundary_border_buffer, candidate_border_route_multipart)
        arcpy.MultipartToSinglepart_management(candidate_border_route_multipart, candidate_border_route)
        ################################################################################################################


        ################################################################################################################
        # Preparation for angle filter and topology analysis
        # Split candidates by boundary offset and boundary-candidate intersections

        # generate offset around boundary
        boundary_border_offset= os.path.join(workspace,"{0}_boundary_tolerance_offset".format(boundary))
        arcpy.Buffer_analysis(boundary_border_dissolved, boundary_border_offset, offset, "FULL", "ROUND", "ALL")

        # get intersections between candidate border route, boundary border, and boundary offset
        # candidate_border_route_boundary_border_intersections = os.path.join(workspace,"candidate_{0}_border_route_{0}_border_intersections".format(boundary))
        # arcpy.Intersect_analysis([candidate_border_route,boundary_border_dissolved], candidate_border_route_boundary_border_intersections, "ALL", "", "point")

        candidate_border_route_boundary_offset_intersections = os.path.join(workspace,"candidate_{0}_border_route_{0}_offset_intersections".format(boundary))
        arcpy.Intersect_analysis([candidate_border_route,boundary_border_offset], candidate_border_route_boundary_offset_intersections, "ALL", "", "point")

        # split candidate border routes
        # boundary_border_within_splitted_tmp = "in_memory\\candidate_{0}_border_route_splitted_tmp".format(boundary)
        # arcpy.SplitLineAtPoint_management(candidate_border_route,candidate_border_route_boundary_border_intersections,\
        #                                   boundary_border_within_splitted_tmp,xy_resolution)
        #
        # candidate_border_route_splitted = os.path.join(workspace,"candidate_{0}_border_route_splitted".format(boundary))
        # arcpy.SplitLineAtPoint_management(boundary_border_within_splitted_tmp,candidate_border_route_boundary_offset_intersections,\
        #                                   candidate_border_route_splitted,xy_resolution)

        candidate_border_route_splitted = os.path.join(workspace,"candidate_{0}_border_route_splitted".format(boundary))
        arcpy.SplitLineAtPoint_management(candidate_border_route,candidate_border_route_boundary_offset_intersections,\
                                          candidate_border_route_splitted,xy_resolution)

        # Add 'SEGMENT_ID_ALL_CANDIDATES' field to candidate route and populate it with 'OBJECTID'
        arcpy.AddField_management(candidate_border_route_splitted,"SEGMENT_ID_CANDIDATES","LONG")
        arcpy.CalculateField_management(candidate_border_route_splitted, "SEGMENT_ID_CANDIDATES", "!OBJECTID!", "PYTHON")

        # Add 'ANGLE_ROUTE' field to candidate route and populate it with the angle to the true north(= 0 degree)
        arcpy.AddField_management(candidate_border_route_splitted,"ANGLE_ROUTE","DOUBLE")
        with arcpy.da.UpdateCursor(candidate_border_route_splitted,("SHAPE@","ANGLE_ROUTE")) as uCur:
            for row in uCur:
                shape = row[0]
                x_first = shape.firstPoint.X
                y_first = shape.firstPoint.Y
                x_last = shape.lastPoint.X
                y_last = shape.lastPoint.Y

                angle = calculate_angle(x_first,y_first,x_last,y_last)
                if angle >=0:
                    row[1]=angle
                    uCur.updateRow(row)
        del uCur
        ################################################################################################################


        ################################################################################################################
        # Apply angle filter
        arcpy.AddMessage("Filtering out candidate border routes that 'intersects' boundary at high angles...")

        candidate_border_route_splitted_buffer = os.path.join(workspace,"candidate_{0}_border_route_splitted_buffer".format(boundary))
        arcpy.Buffer_analysis(candidate_border_route_splitted, candidate_border_route_splitted_buffer, buffer_size, "FULL", "FLAT")

        # # clip boundary segments within route buffer
        # boundary_border_within_splitted_candidate_border_route_buffer_intersect = "in_memory\\{0}_boundary_within_splitted_candidate_{0}_border_route_buffer_intersect".format(boundary)
        # boundary_border_within_splitted_candidate_border_route_buffer = os.path.join(workspace,"{0}_boundary_within_splitted_candidate_{0}_border_route_buffer".format(boundary))
        # arcpy.Clip_analysis(boundary_border_dissolved, candidate_border_route_splitted_buffer, boundary_border_within_splitted_candidate_border_route_buffer_multipart)
        # arcpy.MultipartToSinglepart_management(boundary_border_within_splitted_candidate_border_route_buffer_multipart, boundary_border_within_splitted_candidate_border_route_buffer)

        # get boundary segments within route buffer
        boundary_border_within_splitted_candidate_border_route_buffer_intersect = "in_memory\\{0}_boundary_within_splitted_candidate_{0}_border_route_buffer_intersect".format(boundary)
        boundary_border_within_splitted_candidate_border_route_buffer = os.path.join(workspace,"{0}_boundary_within_splitted_candidate_{0}_border_route_buffer".format(boundary))
        arcpy.Intersect_analysis ([[boundary_border_dissolved, 1], [candidate_border_route_splitted_buffer, 2]],\
                                  boundary_border_within_splitted_candidate_border_route_buffer_intersect, "ALL", "", "INPUT")

        arcpy.Dissolve_management(boundary_border_within_splitted_candidate_border_route_buffer_intersect,\
                                  boundary_border_within_splitted_candidate_border_route_buffer,\
                                  ["SEGMENT_ID_CANDIDATES","ANGLE_ROUTE","CITY_NAME"],"","SINGLE_PART", "UNSPLIT_LINES")

        # Add 'ANGLE_BOUNDARY' field to boundary segment within route buffer and populate it with the angle to the true north(= 0 degree)
        arcpy.AddField_management(boundary_border_within_splitted_candidate_border_route_buffer,"ANGLE_BOUNDARY","DOUBLE")
        with arcpy.da.UpdateCursor(boundary_border_within_splitted_candidate_border_route_buffer,("SHAPE@","ANGLE_BOUNDARY")) as uCur:
            for row in uCur:
                shape = row[0]
                x_first = shape.firstPoint.X
                y_first = shape.firstPoint.Y
                x_last = shape.lastPoint.X
                y_last = shape.lastPoint.Y

                angle = calculate_angle(x_first,y_first,x_last,y_last)

                if angle:
                    row[1]=angle
                    uCur.updateRow(row)
        del uCur

        # TODO: locate boundary border along candidate border route for angle filter analysis generates unexpected result, use spatial join solution instead.
        # # locate boundary segment within buffer along candidate border route.
        # # assuming that if the boundary segment can't be located along its corresponding route, these two might have high angles.
        # boundary_along_splitted_candidate_border_route = os.path.join(workspace,"{0}_boundary_along_splitted_candidate_{0}_border_route".format(boundary))
        # arcpy.LocateFeaturesAlongRoutes_lr(boundary_border_within_splitted_candidate_border_route_buffer,\
        #                                    candidate_border_route_splitted,"SEGMENT_ID_CANDIDATES",buffer_size,\
        #                                    boundary_along_splitted_candidate_border_route,\
        #                                    "{0} {1} {2} {3}".format("RID","LINE","FMEAS","TMEAS"))
        #
        # arcpy.JoinField_management(boundary_along_splitted_candidate_border_route, "RID", candidate_border_route_splitted, "SEGMENT_ID_CANDIDATES", ["ANGLE_ROUTE"])
        #
        #
        # positive_candidate_border_route = []
        # with arcpy.da.SearchCursor(boundary_along_splitted_candidate_border_route,("RID","ANGLE_ROUTE","ANGLE_BOUNDARY")) as sCur:
        #     for row in sCur:
        #         sid = str(row[0])
        #         angle_route = row[1]
        #         angle_boundary = row[2]
        #
        #         # comparing only when angles are valid
        #         if angle_route and angle_boundary:
        #             delta_angle = abs(angle_route-angle_boundary)
        #
        #             # get real intersecting angle
        #             if delta_angle > 90 and delta_angle <= 270:
        #                 delta_angle = abs(180 - delta_angle)
        #             elif delta_angle > 270:
        #                 delta_angle = 360 - delta_angle
        #             else:
        #                 pass
        #
        #             # filter out negative candidate border route
        #             if delta_angle <= high_angle_threshold:
        #                 if sid not in positive_candidate_border_route:
        #                     positive_candidate_border_route.append(sid)
        # del sCur

        # # spatial join boundary border segments within candidate border route buffer to candidate border route
        # # then compare the angle between each pair
        # candidate_border_route_splitted_join_boundary_border = os.path.join(workspace,"candidate_{0}_border_route_join_{0}_boundary_border".format(boundary))
        # arcpy.SpatialJoin_analysis(candidate_border_route_splitted,boundary_border_within_splitted_candidate_border_route_buffer,\
        #                            candidate_border_route_splitted_join_boundary_border,"JOIN_ONE_TO_MANY","","","WITHIN_A_DISTANCE",buffer_size)

        positive_candidate_border_route = []
        negative_candidate_border_route = []
        with arcpy.da.SearchCursor(boundary_border_within_splitted_candidate_border_route_buffer,("SEGMENT_ID_CANDIDATES","ANGLE_ROUTE","ANGLE_BOUNDARY")) as sCur:
            for row in sCur:
                sid = str(row[0])
                angle_route = row[1]
                angle_boundary = row[2]

                # comparing only when angles are valid
                if angle_route and angle_boundary:
                    delta_angle = abs(angle_route-angle_boundary)

                    # get real intersecting angle
                    if delta_angle > 90 and delta_angle <= 270:
                        delta_angle = abs(180 - delta_angle)
                    elif delta_angle > 270:
                        delta_angle = 360 - delta_angle
                    else:
                        pass

                    # filter out negative candidate border route
                    if delta_angle <= high_angle_threshold:
                        if sid not in positive_candidate_border_route:
                            positive_candidate_border_route.append(sid)
                    else:
                        if sid not in negative_candidate_border_route:
                            negative_candidate_border_route.append(sid)
        del sCur

        # TODO: decide what should be included as postive candidates.
        # real_positive_candidate_border_route = list(set(positive_candidate_border_route)-set(negative_candidate_border_route))
        # test = list(set(positive_candidate_border_route)&set(negative_candidate_border_route))
        # for t in test:
        #     arcpy.AddMessage(str(t))

        candidate_border_route_splitted_lyr = "in_memory\\candidate_border_route_splitted_lyr"
        arcpy.MakeFeatureLayer_management(candidate_border_route_splitted, candidate_border_route_splitted_lyr)
        candidate_border_route_splitted_positive = os.path.join(workspace,"candidate_{0}_border_route_splitted_positive".format(boundary))
        where_clause = "\"{0}\" IN ({1})".format("OBJECTID",",".join(positive_candidate_border_route))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_splitted_lyr, "NEW_SELECTION", where_clause)
        arcpy.CopyFeatures_management(candidate_border_route_splitted_lyr,candidate_border_route_splitted_positive)

        candidate_border_route_splitted_negative = os.path.join(workspace,"candidate_{0}_border_route_splitted_negative".format(boundary))
        where_clause = "\"{0}\" NOT IN ({1})".format("OBJECTID",",".join(positive_candidate_border_route))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_splitted_lyr, "NEW_SELECTION", where_clause)
        arcpy.CopyFeatures_management(candidate_border_route_splitted_lyr,candidate_border_route_splitted_negative)
        ################################################################################################################


        ################################################################################################################
        # Topology analysis
        # Get left, right boundary topology of positive candidate border route
        # Handle candidate border route segment with different L/R boundary id by offset
        # TODO: consider to revise topology analysis function to avoid using 'LocateFeatureAlongRoute' function
        arcpy.AddMessage("Calculating L/R boundary topology of positive candidate border route...")

        # get positive candidate border route segments that within boundary offset
        candidate_border_route_positive_within_offset = os.path.join(workspace,"candidate_{0}_border_route_positive_within_offset".format(boundary))
        candidate_border_route_splitted_positive_lyr = "in_memory\\candidate_{0}_border_route_splitted_positive_lyr".format(boundary)
        arcpy.MakeFeatureLayer_management(candidate_border_route_splitted_positive, candidate_border_route_splitted_positive_lyr)
        arcpy.SelectLayerByLocation_management (candidate_border_route_splitted_positive_lyr, "WITHIN", boundary_border_offset)
        arcpy.CopyFeatures_management(candidate_border_route_splitted_positive_lyr,candidate_border_route_positive_within_offset)

        # get positive candidate border route segments that out of boundary offset
        candidate_border_route_positive_outof_offset = os.path.join(workspace,"candidate_{0}_border_route_positive_outof_offset".format(boundary))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_splitted_positive_lyr, "SWITCH_SELECTION")
        arcpy.CopyFeatures_management(candidate_border_route_splitted_positive_lyr,candidate_border_route_positive_outof_offset)

        # generate offset around positive candidate border route within boundary offset
        # arcpy.AddMessage("generate offset around boundary")
        candidate_border_route_positive_within_offset_buffer= os.path.join(workspace,"candidate_{0}_border_route_positive_within_offset_buffer".format(boundary))
        arcpy.Buffer_analysis(candidate_border_route_positive_within_offset, candidate_border_route_positive_within_offset_buffer, offset, "FULL", "FLAT")

        # clip boundary segments within offset distance from positive candidate route that within boundary offset
        boundary_border_within_positive_candidate_border_route_buffer_multipart = "in_memory\\{0}_boundary_within_positive_candidate_border_route_buffer_multipart".format(boundary)
        boundary_border_within_positive_candidate_border_route_buffer = os.path.join(workspace,"{0}_boundary_within_positive_candidate_border_route_buffer".format(boundary))
        arcpy.Clip_analysis(boundary_border_dissolved, candidate_border_route_positive_within_offset_buffer, boundary_border_within_positive_candidate_border_route_buffer_multipart)
        arcpy.MultipartToSinglepart_management(boundary_border_within_positive_candidate_border_route_buffer_multipart, boundary_border_within_positive_candidate_border_route_buffer)

        # get endpoints of boundary border within offset buffer of splitted positive candidate border routes
        boundary_border_within_positive_candidate_border_route_buffer_endpoints = os.path.join(workspace,"{0}_boundary_within_positive_candidate_border_route_buffer_endpoints".format(boundary))
        arcpy.FeatureVerticesToPoints_management(boundary_border_within_positive_candidate_border_route_buffer,\
                                                 boundary_border_within_positive_candidate_border_route_buffer_endpoints,"BOTH_ENDS")
        arcpy.DeleteIdentical_management(boundary_border_within_positive_candidate_border_route_buffer_endpoints, ["Shape"])

        # split boundary border within offset buffer of splitted positive candidate border routes at endpoint location
        # then delete identical shape
        boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints = os.path.join(workspace,"{0}_boundary_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints".format(boundary))
        arcpy.SplitLineAtPoint_management(boundary_border_within_positive_candidate_border_route_buffer,boundary_border_within_positive_candidate_border_route_buffer_endpoints,\
                                          boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints,xy_resolution)
        arcpy.DeleteIdentical_management(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints, ["Shape"])

        # Add 'SEGMENT_ID_BOUNDARY' field to boundary segments within offset distance from positive candidate route that within boundary offset and populate it with 'OBJECTID'
        arcpy.AddField_management(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints,"SEGMENT_ID_BOUNDARY","LONG")
        arcpy.CalculateField_management(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints, "SEGMENT_ID_BOUNDARY", "!OBJECTID!", "PYTHON")

        # locate boundary segments within offset distance of positive candidate route that within boundary offset along positive candidate route that within boundary offset
        boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route = os.path.join(workspace,"{0}_boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route".format(boundary))
        arcpy.LocateFeaturesAlongRoutes_lr(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints,candidate_border_route_positive_within_offset,"SEGMENT_ID_CANDIDATES",offset,\
                                           boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route,"{0} {1} {2} {3}".format("RID","LINE","FMEAS","TMEAS"))

        # get left, right boundary topology of boundary within offset distance of positive candidate route that within boundary offset along positive candidate route that within boundary offset
        boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases= os.path.join(workspace,"{0}_boundary_border_within_positive_candidate_border_route_buffer_with_{1}_topology_allcases".format(boundary,boundary))
        arcpy.Identity_analysis(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints, boundary, boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases,"ALL","","KEEP_RELATIONSHIPS")

        boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr = "in_memory\\{0}_boundary_border_within_positive_candidate_border_route_buffer_with_{1}_topology_allcases_lyr".format(boundary,boundary)
        arcpy.MakeFeatureLayer_management(boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases, boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr)

        where_clause = "\"{0}\"<>0 AND \"{1}\"<>0".format("LEFT_{0}".format(boundary),"RIGHT_{0}".format(boundary))
        arcpy.SelectLayerByAttribute_management(boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr, "NEW_SELECTION", where_clause)
        boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology = os.path.join(workspace,"{0}_boundary_border_within_positive_candidate_border_route_buffer_with_{1}_topology".format(boundary,boundary))
        arcpy.CopyFeatures_management(boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr,boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology)

        arcpy.JoinField_management(boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route,"SEGMENT_ID_BOUNDARY",\
                                   boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology,"SEGMENT_ID_BOUNDARY",["LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field)])

        arcpy.JoinField_management(candidate_border_route_positive_within_offset,"SEGMENT_ID_CANDIDATES",\
                                   boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route,"RID",["SEGMENT_ID_BOUNDARY","LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field)])

        candidate_border_route_positive_within_offset_lyr = "in_memory\\candidate_{0}_border_route_positive_within_offset_lyr".format(boundary)
        arcpy.MakeFeatureLayer_management(candidate_border_route_positive_within_offset, candidate_border_route_positive_within_offset_lyr)
        where_clause = "\"{0}\"IS NOT NULL AND \"{1}\"IS NOT NULL".format("LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_positive_within_offset_lyr, "NEW_SELECTION", where_clause)
        candidate_border_route_positive_within_offset_with_polygon_topology = os.path.join(workspace,"candidate_{0}_border_route_positive_within_offset_with_{1}_topology".format(boundary,boundary))
        arcpy.CopyFeatures_management(candidate_border_route_positive_within_offset_lyr,candidate_border_route_positive_within_offset_with_polygon_topology)

        # get left, right boundary topology of candidate border route out of boundary offset
        candidate_border_route_positive_outof_offset_with_polygon_topology_allcases= os.path.join(workspace,"candidate_{0}_border_route_positive_outof_offset_with_{1}_topology_allcases".format(boundary,boundary))
        arcpy.Identity_analysis(candidate_border_route_positive_outof_offset, boundary, candidate_border_route_positive_outof_offset_with_polygon_topology_allcases,"ALL","","KEEP_RELATIONSHIPS")

        candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr = "in_memory\\candidate_{0}_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr".format(boundary)
        arcpy.MakeFeatureLayer_management(candidate_border_route_positive_outof_offset_with_polygon_topology_allcases, candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr)
        where_clause = "\"{0}\"<>0 AND \"{1}\"<>0".format("LEFT_{0}".format(boundary),"RIGHT_{0}".format(boundary))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr, "NEW_SELECTION", where_clause)
        candidate_border_route_positive_outof_offset_with_polygon_topology = os.path.join(workspace,"candidate_{0}_border_route_positive_outof_offset_with_{1}_topology".format(boundary,boundary))
        arcpy.CopyFeatures_management(candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr,candidate_border_route_positive_outof_offset_with_polygon_topology)

        # merge
        candidate_border_route_positive_with_polygon_topology = "candidate_{0}_border_route_positive_with_{1}_topology".format(boundary,boundary)
        arcpy.FeatureClassToFeatureClass_conversion(candidate_border_route_positive_outof_offset_with_polygon_topology,workspace,candidate_border_route_positive_with_polygon_topology)
        arcpy.Append_management([candidate_border_route_positive_within_offset_with_polygon_topology],candidate_border_route_positive_with_polygon_topology,"NO_TEST")

        ################################################################################################################


        ################################################################################################################
        # Populate route border rule source table
        arcpy.AddMessage("Populating {0} route border rule source table...".format(boundary))

        # calculate from measure and to measure of candidate border route
        # arcpy.AddMessage("Calculating from measure and to measure of candidate border routes...")
        arcpy.AddGeometryAttributes_management(candidate_border_route_positive_with_polygon_topology, "LINE_START_MID_END")

        # get candidte border route segment geometry
        arcpy.AddField_management(candidate_border_route_positive_with_polygon_topology,"SEGMENT_GEOMETRY","TEXT","","",100)
        arcpy.CalculateField_management(candidate_border_route_positive_with_polygon_topology,"SEGMENT_GEOMETRY","!shape.type!","PYTHON")

        # sort candidate border route segments based on route id and from measure, orderly
        # arcpy.AddMessage("sort validated output got above based on route id and from measure, orderly")
        candidate_border_route_positive_with_polygon_topology_sorted = os.path.join(workspace,"candidate_{0}_border_route_positive_with_polygon_topology_sorted".format(boundary))
        arcpy.Sort_management(candidate_border_route_positive_with_polygon_topology,candidate_border_route_positive_with_polygon_topology_sorted,[[route_id_field,"ASCENDING"],["START_M","ASCENDING"]])

        # create route_border_rule_table
        if arcpy.Exists(route_border_rule_table):
            arcpy.Delete_management(route_border_rule_table)
            create_route_border_rule_table_schema(workspace,route_border_rule_table)
        else:
            create_route_border_rule_table_schema(workspace,route_border_rule_table)

        # populate route_border_rule_table
        iCur = arcpy.da.InsertCursor(route_border_rule_table,["ROUTE_ID","ROUTE_START_MEASURE","ROUTE_END_MEASURE","BOUNDARY_LEFT_ID",\
                                                              "BOUNDARY_RIGHT_ID","SEGMENT_GEOMETRY","EFFECTIVE_FROM_DT","EFFECTIVE_TO_DT"])
        with arcpy.da.SearchCursor(candidate_border_route_positive_with_polygon_topology_sorted,[route_id_field,"START_M","END_M","LEFT_{0}".format(boundary_id_field),\
                                                                              "RIGHT_{0}".format(boundary_id_field),"SEGMENT_GEOMETRY","START_DATE","END_DATE"]) as sCur:
            for row in sCur:
                iCur.insertRow(row)

        del sCur
        del iCur

        arcpy.CalculateField_management(route_border_rule_table, "BRP_PROCESS_DT", "'{0}'".format(date_string), "PYTHON")
        ###############################################################################################################

        arcpy.AddMessage("done!")

        return route_border_rule_table
    except Exception:
        # arcpy.AddMessage(traceback.format_exc())
        sys.exit(traceback.format_exc())
        return False


def create_route_border_rule_table_schema(workspace,route_border_rule_table):
    # create table
    arcpy.CreateTable_management(workspace,route_border_rule_table)

    # add fields
    field_length = 100
    field_scale = 6
    arcpy.AddField_management(route_border_rule_table,"ROUTE_ID","TEXT","","",field_length)
    arcpy.AddField_management(route_border_rule_table,"ROUTE_START_MEASURE","DOUBLE","",field_scale)
    arcpy.AddField_management(route_border_rule_table,"ROUTE_END_MEASURE","DOUBLE","",field_scale)
    arcpy.AddField_management(route_border_rule_table,"BOUNDARY_LEFT_ID","TEXT","","",field_length)
    arcpy.AddField_management(route_border_rule_table,"BOUNDARY_RIGHT_ID","TEXT","","",field_length)
    arcpy.AddField_management(route_border_rule_table,"SEGMENT_GEOMETRY","TEXT","","",field_length)
    arcpy.AddField_management(route_border_rule_table,"EFFECTIVE_FROM_DT","DATE")
    arcpy.AddField_management(route_border_rule_table,"EFFECTIVE_TO_DT","DATE")
    arcpy.AddField_management(route_border_rule_table,"BRP_PROCESS_DT","DATE")


def calculate_angle(x_first,y_first,x_last,y_last):
    try:
        delta_x = x_last - x_first
        delta_y = y_last - y_first

        angle = -1

        if delta_x == 0 and delta_y > 0:
            angle = 0
        elif delta_x > 0:
            angle = 90 - math.degrees(math.atan(delta_y/delta_x))
        elif delta_x == 0 and delta_y < 0:
            angle = 180
        elif delta_x < 0:
            angle = 270 - math.degrees(math.atan(delta_y/delta_x))

        return angle
    except Exception:
        # arcpy.AddMessage(traceback.format_exc())
        sys.exit(traceback.format_exc())


def get_parameters():
    current_directory = os.path.dirname(os.path.realpath(__file__))
    import ConfigParser
    config = ConfigParser.ConfigParser()
    config.read(os.path.join(current_directory, "configuration.ini"))

    return config


def setup_logger():
    """
    Setup the logger.
    Log file will be generated at the same folder of the script
    @return:
    """

    output_path = os.path.dirname(os.path.realpath(__file__))
    log_path = os.path.join(output_path, "tss.log")

    logging.handlers.AgsHandler = AgsLogHandler

    logging.config.dictConfig({
        "version": 1,
        "disable_existing_loggers": False,  # this fixes the problem

        "formatters": {
            "standard": {
                "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
            },
        },
        "handlers": {
            "default": {
                "level":"INFO",
                "class":"logging.StreamHandler",
                "formatter": "standard",
                "stream": "ext://sys.stdout"
            },
            "info_file": {
                "level": "INFO",
                "class": "logging.handlers.RotatingFileHandler",
                "formatter": "standard",
                "filename": log_path,
                "encoding": "utf8",
                "maxBytes": 10485760,
                "backupCount": 10
            },
            "ags": {
                "level":"INFO",
                "class":"logging.handlers.AgsHandler",
                "formatter": "standard"
            },
        },
        "loggers": {
            "": {
                "handlers": ["default", "info_file", "ags"],
                "level": "INFO"
            }
        }
    })


class AgsLogHandler(logging.Handler):  # Inherit from logging.Handler

    def __init__(self):
        # run the regular Handler __init__
        logging.Handler.__init__(self)

    def emit(self, record):
        # record.message is the log message
        if record.levelname == "INFO":
            arcpy.AddMessage(record.message)
        elif record.levelname == "ERROR":
            arcpy.AddError(record.message)
        elif record.levelname == "WARNING":
            arcpy.AddWarning(record.message)
        else:
            arcpy.AddMessage(record.message)


if __name__ == "__main__":
    main()