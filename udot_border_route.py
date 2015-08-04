__author__ = 'wzhang@tss'

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
    target_db = config.get(section, "TARGET_GB")
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

    # check if source data exists. Copy it from source database if not.
    logger.info("Checking source data existence...")
    source_data = [route]+boundaries
    for data in source_data:
        if not check_source_data(workspace,target_db,data):
            logger.info("'{0}' can not be found in {1}. Please make sure all source data exist before continue...".format(data,workspace))
            logger.info("Task failed at {0}".format(datetime.now().strftime("%m/%d/%Y %H:%M:%S")))
            sys.exit()

    # generate route border rule source tables
    logger.info("All source data exist. Start to generate border route source table.")
    # boundary = boundaries[0]
    # boundary_id_field = boundaries_id_fields[0]
    # route_border_rule_table = route_border_rule_tables[0]
    # if not generate_route_border_rule_table(workspace,target_db,route,route_id_field,boundary,boundary_id_field,buffer_size,route_border_rule_table,high_angle_threshold,offset):
    #     logger.warning("Failed to generate border route rule source table for '{0}' feature...".format(boundary))
    #     logger.info("Task failed at {0}".format(datetime.now().strftime("%m/%d/%Y %H:%M:%S")))
    #     sys.exit()

    for boundary in boundaries:
        index = boundaries.index(boundary)
        boundary_id_field = boundaries_id_fields[index]
        route_border_rule_table = route_border_rule_tables[index]

        if not generate_route_border_rule_table(workspace,target_db,route,route_id_field,boundary,boundary_id_field,buffer_size,route_border_rule_table,high_angle_threshold,offset):
            logger.warning("Failed to generate border route rule source table for '{0}' feature...".format(boundary))
            logger.info("Task failed at {0}".format(datetime.now().strftime("%m/%d/%Y %H:%M:%S")))
            sys.exit()


def check_source_data(workspace,target_db,data):
    try:
        if not arcpy.Exists(os.path.join(workspace,data)):
            logger.info("{0} does not exist in {1}. Copying {0} from source database.".format(data,workspace))
            arcpy.FeatureClassToFeatureClass_conversion(os.path.join(target_db,data),workspace,data)
        return True
    except Exception:
        logger.warning(traceback.format_exc())
        return False


def generate_route_border_rule_table(workspace,target_db,route,route_id_field,boundary,boundary_id_field,buffer_size,route_border_rule_table,high_angle_threshold,offset):
    logger.info("Generating route border rule source table for {0}...".format(boundary))
    try:
        dir_path = os.path.dirname(os.path.realpath(__file__))
        scratch_workspace = os.path.join(dir_path,"scratch.gdb")
        if not arcpy.Exists(scratch_workspace):
            arcpy.CreateFileGDB_management(dir_path, "scratch.gdb")

        arcpy.env.workspace = workspace
        arcpy.env.scratchWorkspace = scratch_workspace
        arcpy.env.overwriteOutput = True

        date = datetime.now()
        date_string = date.strftime("%m/%d/%Y")

        spatial_reference = arcpy.Describe(route).spatialReference
        xy_resolution = "{0} {1}".format(spatial_reference.XYResolution,spatial_reference.linearUnitName)

        ###############################################################################################################
        # get all candidate border routes
        logger.info("Identifying candidate border routes...")

        # generate boundary border
        boundary_border = os.path.join(scratch_workspace,"{0}_{1}_border".format(boundary,"boundary"))
        arcpy.FeatureToLine_management(boundary, boundary_border)

        # dissolve polygon boundary based on boundary id
        boundary_border_dissolved = os.path.join(scratch_workspace,"{0}_boundary_border_dissolved".format(boundary))
        arcpy.Dissolve_management(boundary_border,boundary_border_dissolved,[boundary_id_field])

        # generate buffer around boundary
        # logger.info("generate buffer around boundary")
        boundary_border_buffer = os.path.join(scratch_workspace,"{0}_{1}".format(boundary,"boundary_buffer"))
        arcpy.Buffer_analysis(boundary_border_dissolved, boundary_border_buffer, buffer_size, "FULL", "ROUND")

        # get candidate border route
        # logger.info("get candidate border route")
        candidate_border_route_multipart = "in_memory\\candidate_{0}_border_route_multipart".format(boundary)
        candidate_border_route = os.path.join(scratch_workspace,"candidate_{0}_border_route".format(boundary))
        arcpy.Clip_analysis(route, boundary_border_buffer, candidate_border_route_multipart)
        arcpy.MultipartToSinglepart_management(candidate_border_route_multipart, candidate_border_route)
        ################################################################################################################


        ################################################################################################################
        #  filter out candidate border routes that 'intersects' boundary at high angles
        logger.info("Filtering out candidate border routes that 'intersects' boundary at high angles...")

        route_buffer = os.path.join(scratch_workspace,"{0}_{1}".format(route,"buffer_flat"))
        if not arcpy.Exists(route_buffer):
            arcpy.Buffer_analysis(route, route_buffer, buffer_size, "FULL", "FLAT")

        # clip boundary segments within route buffer
        boundary_border_within_buffer_multipart = "in_memory\\{0}_boundary_within_{1}_buffer_multipart".format(boundary,route)
        boundary_border_within_buffer = os.path.join(scratch_workspace,"{0}_boundary_within_{1}_buffer".format(boundary,route))
        arcpy.Clip_analysis(boundary_border_dissolved, route_buffer, boundary_border_within_buffer_multipart)
        arcpy.MultipartToSinglepart_management(boundary_border_within_buffer_multipart, boundary_border_within_buffer)

        # Add 'SEGMENT_ID_ALL_CANDIDATES' field to candidate route and populate it with 'OBJECTID'
        arcpy.AddField_management(candidate_border_route,"SEGMENT_ID_ALL_CANDIDATES","LONG")
        arcpy.CalculateField_management(candidate_border_route, "SEGMENT_ID_ALL_CANDIDATES", "!OBJECTID!", "PYTHON")

        # Add 'ANGLE_ROUTE' field to candidate route and populate it with the angle to the true north(= 0 degree)
        arcpy.AddField_management(candidate_border_route,"ANGLE_ROUTE","DOUBLE")
        with arcpy.da.UpdateCursor(candidate_border_route,("SHAPE@","ANGLE_ROUTE")) as uCur:
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

        # Add 'ANGLE_BOUNDARY' field to boundary segment within route buffer and populate it with the angle to the true north(= 0 degree)
        arcpy.AddField_management(boundary_border_within_buffer,"ANGLE_BOUNDARY","DOUBLE")
        with arcpy.da.UpdateCursor(boundary_border_within_buffer,("SHAPE@","ANGLE_BOUNDARY")) as uCur:
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

        # locate boundary segment within buffer along candidate border route.
        # assuming that if the boundary segment can't be located along its corresponding route, these two might have high angles.
        boundary_along_candidate_border_route = os.path.join(scratch_workspace,"{0}_boundary_along_candidate_{1}_border_route".format(boundary,boundary))
        arcpy.LocateFeaturesAlongRoutes_lr(boundary_border_within_buffer,candidate_border_route,"SEGMENT_ID_ALL_CANDIDATES",buffer_size,\
                                           boundary_along_candidate_border_route,"{0} {1} {2} {3}".format("RID","LINE","FMEAS","TMEAS"))

        arcpy.JoinField_management(boundary_along_candidate_border_route, "RID", candidate_border_route, "SEGMENT_ID_ALL_CANDIDATES", ["ANGLE_ROUTE"])


        positive_candidate_border_route = []
        with arcpy.da.SearchCursor(boundary_along_candidate_border_route,("RID","ANGLE_ROUTE","ANGLE_BOUNDARY")) as sCur:
            for row in sCur:
                sid = str(row[0])
                angle_route = row[1]
                angle_boundary = row[2]

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
        del sCur

        candidate_border_route_lyr = "in_memory\\candidate_border_route_lyr"
        arcpy.MakeFeatureLayer_management(candidate_border_route, candidate_border_route_lyr)
        candidate_border_route_positive = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive".format(boundary))
        where_clause = "\"{0}\" IN ({1})".format("OBJECTID",",".join(positive_candidate_border_route))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_lyr, "NEW_SELECTION", where_clause)
        arcpy.CopyFeatures_management(candidate_border_route_lyr,candidate_border_route_positive)

        candidate_border_route_negative = os.path.join(scratch_workspace,"candidate_{0}_border_route_negative".format(boundary))
        where_clause = "\"{0}\" NOT IN ({1})".format("OBJECTID",",".join(positive_candidate_border_route))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_lyr, "NEW_SELECTION", where_clause)
        arcpy.CopyFeatures_management(candidate_border_route_lyr,candidate_border_route_negative)
        ################################################################################################################


        ################################################################################################################
        # get left, right boundary topology of positive candidate border route
        # handle candidate border route segment with different L/R boundary id by offset
        logger.info("Calculating L/R boundary topology of positive candidate border route...")

        # generate offset around boundary
        boundary_border_offset= os.path.join(scratch_workspace,"{0}_{1}".format(boundary,"boundary_offset"))
        arcpy.Buffer_analysis(boundary_border_dissolved, boundary_border_offset, offset, "FULL", "ROUND")

        # get intersections between positive candidate border route and boundary offset
        candidate_border_route_positive_boundary_offset_intersections = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_{1}_offset_intersections".format(boundary,boundary))
        arcpy.Intersect_analysis([candidate_border_route_positive,boundary_border_offset], candidate_border_route_positive_boundary_offset_intersections, "ALL", "", "point")

        # split positive candidate border route by intersections generated above
        candidate_border_route_positive_splitted_by_offset = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_splitted_by_offset".format(boundary))
        arcpy.SplitLineAtPoint_management(candidate_border_route_positive,candidate_border_route_positive_boundary_offset_intersections,\
                                          candidate_border_route_positive_splitted_by_offset,xy_resolution)

        # Add 'SEGMENT_ID_POSITIVE_CANDIDATES' field to splitted positive candidate route and populate it with 'OBJECTID'
        arcpy.AddField_management(candidate_border_route_positive_splitted_by_offset,"SEGMENT_ID_POSITIVE_CANDIDATES","LONG")
        arcpy.CalculateField_management(candidate_border_route_positive_splitted_by_offset, "SEGMENT_ID_POSITIVE_CANDIDATES", "!OBJECTID!", "PYTHON")

        # get positive candidate border route segments that within boundary offset
        candidate_border_route_positive_within_offset = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_within_offset".format(boundary))
        candidate_border_route_positive_splitted_by_offset_lyr = "in_memory\\candidate_{0}_border_route_positive_splitted_by_offset_lyr".format(boundary)
        arcpy.MakeFeatureLayer_management(candidate_border_route_positive_splitted_by_offset, candidate_border_route_positive_splitted_by_offset_lyr)
        arcpy.SelectLayerByLocation_management (candidate_border_route_positive_splitted_by_offset_lyr, "WITHIN", boundary_border_offset)
        arcpy.CopyFeatures_management(candidate_border_route_positive_splitted_by_offset_lyr,candidate_border_route_positive_within_offset)

        # get positive candidate border route segments that out of boundary offset
        candidate_border_route_positive_outof_offset = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_outof_offset".format(boundary))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_positive_splitted_by_offset_lyr, "SWITCH_SELECTION")
        arcpy.CopyFeatures_management(candidate_border_route_positive_splitted_by_offset_lyr,candidate_border_route_positive_outof_offset)

        # generate offset around positive candidate border route within boundary offset
        # logger.info("generate offset around boundary")
        candidate_border_route_positive_within_offset_buffer= os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_within_offset_buffer".format(boundary))
        arcpy.Buffer_analysis(candidate_border_route_positive_within_offset, candidate_border_route_positive_within_offset_buffer, offset, "FULL", "FLAT")

        # clip boundary segments within offset distance from positive candidate route that within boundary offset
        boundary_border_within_positive_candidate_border_route_buffer_multipart = "in_memory\\{0}_boundary_within_positive_candidate_border_route_buffer_multipart".format(boundary)
        boundary_border_within_positive_candidate_border_route_buffer = os.path.join(scratch_workspace,"{0}_boundary_within_positive_candidate_border_route_buffer".format(boundary))
        arcpy.Clip_analysis(boundary_border_dissolved, candidate_border_route_positive_within_offset_buffer, boundary_border_within_positive_candidate_border_route_buffer_multipart)
        arcpy.MultipartToSinglepart_management(boundary_border_within_positive_candidate_border_route_buffer_multipart, boundary_border_within_positive_candidate_border_route_buffer)

        # get endpoints of boundary border within offset buffer of splitted positive candidate border routes
        boundary_border_within_positive_candidate_border_route_buffer_endpoints = os.path.join(scratch_workspace,"{0}_boundary_within_positive_candidate_border_route_buffer_endpoints".format(boundary))
        arcpy.FeatureVerticesToPoints_management(boundary_border_within_positive_candidate_border_route_buffer,\
                                                 boundary_border_within_positive_candidate_border_route_buffer_endpoints,"BOTH_ENDS")
        arcpy.DeleteIdentical_management(boundary_border_within_positive_candidate_border_route_buffer_endpoints, ["Shape"])

        # split boundary border within offset buffer of splitted positive candidate border routes and endpoints location
        # then delete identical shape
        boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints = os.path.join(scratch_workspace,"{0}_boundary_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints".format(boundary))
        arcpy.SplitLineAtPoint_management(boundary_border_within_positive_candidate_border_route_buffer,boundary_border_within_positive_candidate_border_route_buffer_endpoints,\
                                          boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints,xy_resolution)
        arcpy.DeleteIdentical_management(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints, ["Shape"])

        # Add 'SEGMENT_ID_BOUNDARY' field to boundary segments within offset distance from positive candidate route that within boundary offset and populate it with 'OBJECTID'
        arcpy.AddField_management(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints,"SEGMENT_ID_BOUNDARY","LONG")
        arcpy.CalculateField_management(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints, "SEGMENT_ID_BOUNDARY", "!OBJECTID!", "PYTHON")

        # locate boundary segments within offset distance of positive candidate route that within boundary offset along positive candidate route that within boundary offset
        boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route = os.path.join(scratch_workspace,"{0}_boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route".format(boundary))
        arcpy.LocateFeaturesAlongRoutes_lr(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints,candidate_border_route_positive_within_offset,"SEGMENT_ID_POSITIVE_CANDIDATES",offset,\
                                           boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route,"{0} {1} {2} {3}".format("RID","LINE","FMEAS","TMEAS"))

        # get left, right boundary topology of boundary within offset distance of positive candidate route that within boundary offset along positive candidate route that within boundary offset
        boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases= os.path.join(scratch_workspace,"{0}_boundary_border_within_positive_candidate_border_route_buffer_with_{1}_topology_allcases".format(boundary,boundary))
        arcpy.Identity_analysis(boundary_border_within_positive_candidate_border_route_buffer_splitted_by_own_endpoints, boundary, boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases,"ALL","","KEEP_RELATIONSHIPS")

        boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr = "in_memory\\{0}_boundary_border_within_positive_candidate_border_route_buffer_with_{1}_topology_allcases_lyr".format(boundary,boundary)
        arcpy.MakeFeatureLayer_management(boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases, boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr)

        where_clause = "\"{0}\"<>0 AND \"{1}\"<>0".format("LEFT_{0}".format(boundary),"RIGHT_{0}".format(boundary))
        arcpy.SelectLayerByAttribute_management(boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr, "NEW_SELECTION", where_clause)
        boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology = os.path.join(scratch_workspace,"{0}_boundary_border_within_positive_candidate_border_route_buffer_with_{1}_topology".format(boundary,boundary))
        arcpy.CopyFeatures_management(boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology_allcases_lyr,boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology)

        arcpy.JoinField_management(boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route,"SEGMENT_ID_BOUNDARY",\
                                   boundary_border_within_positive_candidate_border_route_buffer_with_polygon_topology,"SEGMENT_ID_BOUNDARY",["LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field)])

        arcpy.JoinField_management(candidate_border_route_positive_within_offset,"SEGMENT_ID_POSITIVE_CANDIDATES",\
                                   boundary_border_within_positive_candidate_border_route_buffer_along_candidate_border_route,"RID",["SEGMENT_ID_BOUNDARY","LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field)])

        candidate_border_route_positive_within_offset_lyr = "in_memory\\candidate_{0}_border_route_positive_within_offset_lyr".format(boundary)
        arcpy.MakeFeatureLayer_management(candidate_border_route_positive_within_offset, candidate_border_route_positive_within_offset_lyr)
        where_clause = "\"{0}\"IS NOT NULL AND \"{1}\"IS NOT NULL".format("LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_positive_within_offset_lyr, "NEW_SELECTION", where_clause)
        candidate_border_route_positive_within_offset_with_polygon_topology = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_within_offset_with_{1}_topology".format(boundary,boundary))
        arcpy.CopyFeatures_management(candidate_border_route_positive_within_offset_lyr,candidate_border_route_positive_within_offset_with_polygon_topology)

        # get left, right boundary topology of candidate border route out of boundary offset
        candidate_border_route_positive_outof_offset_with_polygon_topology_allcases= os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_outof_offset_with_{1}_topology_allcases".format(boundary,boundary))
        arcpy.Identity_analysis(candidate_border_route_positive_outof_offset, boundary, candidate_border_route_positive_outof_offset_with_polygon_topology_allcases,"ALL","","KEEP_RELATIONSHIPS")

        candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr = "in_memory\\candidate_{0}_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr".format(boundary)
        arcpy.MakeFeatureLayer_management(candidate_border_route_positive_outof_offset_with_polygon_topology_allcases, candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr)
        where_clause = "\"{0}\"<>0 AND \"{1}\"<>0".format("LEFT_{0}".format(boundary),"RIGHT_{0}".format(boundary))
        arcpy.SelectLayerByAttribute_management(candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr, "NEW_SELECTION", where_clause)
        candidate_border_route_positive_outof_offset_with_polygon_topology = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_outof_offset_with_{1}_topology".format(boundary,boundary))
        arcpy.CopyFeatures_management(candidate_border_route_positive_outof_offset_with_polygon_topology_allcases_lyr,candidate_border_route_positive_outof_offset_with_polygon_topology)

        # merge
        candidate_border_route_positive_with_polygon_topology = "candidate_{0}_border_route_positive_with_{1}_topology".format(boundary,boundary)
        arcpy.FeatureClassToFeatureClass_conversion(candidate_border_route_positive_outof_offset_with_polygon_topology,scratch_workspace,candidate_border_route_positive_with_polygon_topology)
        candidate_border_route_positive_with_polygon_topology = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_with_{1}_topology".format(boundary,boundary))
        arcpy.Append_management([candidate_border_route_positive_within_offset_with_polygon_topology],candidate_border_route_positive_with_polygon_topology,"NO_TEST")
        ################################################################################################################


        ################################################################################################################
        logger.info("Populate route_border_rule_table...")

        # calculate from measure and to measure of candidate border route
        # logger.info("Calculating from measure and to measure of candidate border routes...")
        arcpy.AddGeometryAttributes_management(candidate_border_route_positive_with_polygon_topology, "LINE_START_MID_END")

        # get candidte border route segment geometry
        arcpy.AddField_management(candidate_border_route_positive_with_polygon_topology,"SEGMENT_GEOMETRY","TEXT","","",100)
        arcpy.CalculateField_management(candidate_border_route_positive_with_polygon_topology,"SEGMENT_GEOMETRY","!shape.type!","PYTHON")

        # sort candidate border route segments based on route id and from measure, orderly
        # logger.info("sort validated output got above based on route id and from measure, orderly")
        candidate_border_route_positive_with_polygon_topology_sorted = os.path.join(scratch_workspace,"candidate_{0}_border_route_positive_with_polygon_topology_sorted".format(boundary))
        arcpy.Sort_management(candidate_border_route_positive_with_polygon_topology,candidate_border_route_positive_with_polygon_topology_sorted,[[route_id_field,"ASCENDING"],["START_M","ASCENDING"]])

        arcpy.AddField_management(candidate_border_route_positive_with_polygon_topology_sorted,"BRP_PROCESS_DT","DATE")
        arcpy.CalculateField_management(candidate_border_route_positive_with_polygon_topology_sorted, "BRP_PROCESS_DT", "'{0}'".format(date_string), "PYTHON")

        # create route_border_rule_fc
        input_fields = [route_id_field,"START_M","END_M","LEFT_{0}".format(boundary_id_field),"RIGHT_{0}".format(boundary_id_field),"SEGMENT_GEOMETRY","START_DATE","END_DATE","BRP_PROCESS_DT"]
        output_fields = ["ROUTE_ID","ROUTE_START_MEASURE","ROUTE_END_MEASURE","BOUNDARY_LEFT_ID","BOUNDARY_RIGHT_ID","SEGMENT_GEOMETRY","EFFECTIVE_FROM_DT","EFFECTIVE_TO_DT","BRP_PROCESS_DT"]

        fms = arcpy.FieldMappings()
        for field in input_fields:
            index = input_fields.index(field)
            output_field = output_fields[index]

            fm = arcpy.FieldMap()
            fm.addInputField(candidate_border_route_positive_with_polygon_topology_sorted,field)
            fm_name = fm.outputField
            fm_name.name = output_field
            fm_name.aliasName = output_field
            fm.outputField = fm_name

            fms.addFieldMap(fm)

        arcpy.FeatureClassToFeatureClass_conversion(candidate_border_route_positive_with_polygon_topology_sorted,\
                                                    workspace,route_border_rule_table,field_mapping=fms)

        fms_target= arcpy.FieldMappings()
        for field in output_fields:
            fm = arcpy.FieldMap()
            fm.addInputField(route_border_rule_table,field)
            fms_target.addFieldMap(fm)

        # export route_border_rule_source table to target database
        arcpy.FeatureClassToFeatureClass_conversion(route_border_rule_table,target_db,route_border_rule_table,field_mapping=fms_target,config_keyword="SDO_GEOMETRY")

        # # create route_border_rule_table
        # if arcpy.Exists(route_border_rule_table):
        #     arcpy.Delete_management(route_border_rule_table)
        #     create_route_border_rule_table_schema(workspace,route_border_rule_table)
        # else:
        #     create_route_border_rule_table_schema(workspace,route_border_rule_table)
        #
        # # populate route_border_rule_table
        # iCur = arcpy.da.InsertCursor(route_border_rule_table,["ROUTE_ID","ROUTE_START_MEASURE","ROUTE_END_MEASURE","BOUNDARY_LEFT_ID",\
        #                                                       "BOUNDARY_RIGHT_ID","SEGMENT_GEOMETRY","EFFECTIVE_FROM_DT","EFFECTIVE_TO_DT"])
        # with arcpy.da.SearchCursor(candidate_border_route_positive_with_polygon_topology_sorted,[route_id_field,"START_M","END_M","LEFT_{0}".format(boundary_id_field),\
        #                                                                       "RIGHT_{0}".format(boundary_id_field),"SEGMENT_GEOMETRY","START_DATE","END_DATE"]) as sCur:
        #     for row in sCur:
        #         iCur.insertRow(row)
        #
        # del sCur
        # del iCur
        #
        # arcpy.CalculateField_management(route_border_rule_table, "BRP_PROCESS_DT", "'{0}'".format(date_string), "PYTHON")
        ################################################################################################################

        logger.info("Finish generating {0}.".format(route_border_rule_table))

        return route_border_rule_table
    except Exception:
        logger.warning(traceback.format_exc())
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
        # logger.info(traceback.format_exc())
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