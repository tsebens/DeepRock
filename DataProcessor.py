# Table Processor 2.0
import sys
import os
import os.path
import time
import math
from collections import Counter
import arcpy
import multiprocessing as mp
import datetime

# import statistics as stats
from collections import Counter
# from KNearestNeighborModel import KNNModel

NAD1983_TO_AkAlb_Transformation = "PROJCS['NAD_1983_Alaska_Albers',GEOGCS['GCS_North_American_1983',DATUM['D_North_American_1983',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]],PROJECTION['Albers'],PARAMETER['False_Easting',0.0],PARAMETER['False_Northing',0.0],PARAMETER['Central_Meridian',-154.0],PARAMETER['Standard_Parallel_1',55.0],PARAMETER['Standard_Parallel_2',65.0],PARAMETER['Latitude_Of_Origin',50.0],UNIT['Meter',1.0]]"

class DataProcessor( object ):

	def __init__( self, verbose ):
		self.verbose = verbose

	def printIfVerbose( self, message ):
		if self.verbose == True:
			print( message )

	def addPercentiles( self ):
		self.printIfVerbose( "addPercentiles not implemented." )
		
	def addResiduals( self ):
		self.printIfVerbose( "addResiduals not implemented." )

	def addXYZData( self ):
		self.printIfVerbose( "addXYData not implemented.")

class FieldNotPresentException( Exception ):
	def __init__( self, table, field ):
		self.table = table
		self.field = field
		
	def __str__ ( self ):
		str = "Field referenced ('%s') which does not exist in table %s" % ( self.field, self.table )
		return str
		
"""
Class which wraps a single File Geodatabase (and dataset within, if specified) and provides functions that process the Feature Classes inside. All processing is based on a Table Processing Record, a table provided within the FGDB. If none exists, one is created, and the processor assumes no processing has occured for any of the classes within.
"""
# TODO: Current implementation assumes the existance of a TPR. Create protocol for no existance
class ArcGDBDataProcessor( DataProcessor ):
	def __init__( self, FGDB, TPR='Table_Processing_Record', dataset=None, verbose=False, multiprocessing_on=False, free_cores=4 ):
		self.verbose = verbose
		self.GDB = FGDB
		self.dataset = dataset
		self.TPR = os.path.join( self.GDB, TPR )
		self.err_log_fp = r"N:\Python Scripts\BathymetryProcessor\2.0\ErrorLogs\proc_err_log.txt"# TODO: Make the location of this log dyanmically defined to an intelligent location
		self.initializeErrorLogHeader()
		self.multiprocessing_on = multiprocessing_on
		self.max_num_cpu = mp.cpu_count() - free_cores
		# Define workspace
		self.WRKSPC = self.GDB
		self.proc_dict = self.defineProcessingDictionary()
		if self.dataset != None:
			self.WRKSPC = os.path.join( self.GDB, self.dataset )
		arcpy.env.workspace = self.WRKSPC
	
	def initializeErrorLogHeader( self ):
		dt = datetime.datetime.now().strftime("%I:%M%p on %B %d, %Y")
		log = open( self.err_log_fp, 'a' )
		log.write( "\n\n----------------------------------------------------------------------------------\nError Log for bathymetry data processing begun at %s.\n----------------------------------------------------------------------------------\n" % dt )
		log.close()
	
	def getTableFields( self, table ):
		fields = [f.name for f in arcpy.ListFields( table )]
		return fields
	
	def getTablesInTPR( self ):
		sCur = arcpy.SearchCursor( self.TPR )
		names = [row.getValue( 'tbl_name' ) for row in sCur]
		return names
	
	def updateTPRWithExistingFCs( self ):
		fcs_in_tpr = self.getTablesInTPR()
		fcs_in_gdb = arcpy.ListFeatureClasses()
		fcs_not_in_tpr = list()
		for fc in fcs_in_gdb:
			if fc not in fcs_in_tpr:
				fcs_not_in_tpr.append( fc )
		# fcs_not_in_tpr now has the names of all of the fcs in the gdb not present in the tpr.'
		
	def determineTableProcessingExtent( self, table ):
		fields = self.getTableFields( table )
		proc_ext_dict = { 'has_x':False, 'has_y':False, 'has_z':False, 'perc':False, 'is_proj':False }
	
	# A method which accepts a processing field from the table processing record as a parameter, and then returns a list of all tables in the tpr which have not undergone that process
	def selectTablesByProcessingRecord( self, process ):
		self.printIfVerbose( "Selecting tables by '%s' field" % process )
		fields = self.getTableFields( self.TPR )
		if process not in fields:
			raise FieldNotPresentException( self.TPR, process )
		
		where_clause = "%s = 0" % process
		spatial_reference = ""
		fields = ""
		sort_fields = ""
		sCur = arcpy.SearchCursor( self.TPR, where_clause, spatial_reference, fields, sort_fields  )
		names = [row.getValue( 'tbl_name' ) for row in sCur]
		del sCur
		return names
	
	# Method that updates the table processing record. There are two copies of this function right now. If they get out of sync, there could be problems
	# TODO: Kind of a big one. Implement a TPR class which accepts as a parameter the name of the TPR, and provides a bunch of common functions for interacting with the TPR
	def updateTableProcessingRecord( self, table, update_fields, update_values ):
		where_clause = "tbl_name = '%s'" % table
		uCur = arcpy.UpdateCursor( self.TPR, where_clause )
		row = uCur.next()
		for index in range( 0, len( update_fields ) ):
			self.printIfVerbose( "Updating %s to %s for %s." % ( update_fields[index], update_values[index], table ) )
			row.setValue( update_fields[index], update_values[index] )
		uCur.updateRow( row )
		del uCur
		del row
	
	def calculateTableSize( self, table ):
		result = arcpy.GetCount_management( table )
		size = int( result.getOutput( 0 ) )
		self.updateTableProcessingRecord( table, ['tbl_size',], [size,] )
			
	def calculateTableStatistics( self, table ):
		# First we need to figure out which column index to look in for the z data
		fields = [f.name for f in arcpy.ListFields( table )]
		search_field = 'z'
		field_index = fields.index( search_field )
		sCur = arcpy.da.SearchCursor( table, search_field )
		z_data = [float( row[0] ) for row in sCur]
		# Calculate the classic aggregate stats
		mean = sum( z_data ) / len( z_data )
		median = sorted( z_data )[len( z_data ) / 2]
		count_data = Counter( z_data )
		mode = count_data.most_common(1)[0][0]  # Returns the highest occurring item
		var = sum([ ( mean - z )**2.0 for z in z_data])/len( z_data )
		std_dev = math.sqrt( var )
		del sCur
		del row
		update_fields = [ 'tbl_std_dev', 'tbl_mean', 'tbl_med', 'tbl_mode' ]
		update_values = [ std_dev, mean, median, mode ]
		self.updateTableProcessingRecord( table, update_fields, update_values )
		
	# TODO: arcpy.da.UpdateCursor is supposed to be faster. Try and work that into the code
	def addPercentiles( self, table ):
		self.printIfVerbose( "Adding percentiles to %s." % table )
		# Determine table size
		result = arcpy.GetCount_management( table )
		size = int( result.getOutput( 0 ) )

		# Add the percentile field to the table
		new_field = 'percentile'
		new_field_type = 'LONG'
		arcpy.AddField_management( table, new_field, new_field_type, "", "", "", "", "", "", "" )
		fields = [f.name for f in arcpy.ListFields( table )]
		# Sort entries by depth (z)
		sort_field = 'z'
		prefix = ""
		postfix = "ORDER BY %s DESC" % sort_field
		sort_clauses = ( prefix, postfix )
		uCur = arcpy.da.UpdateCursor( table, "*", "", "", "", sort_clauses )
		index = 0
		for row in uCur:
			perc = int( ( float( index ) / float( size ) ) * 100.0 )
			row[fields.index( new_field )] = perc
			uCur.updateRow( row )
			index += 1
		del uCur
		del row
		self.updateTableProcessingRecord( table, ['has_perc',], [1,] )

	# Calculates residual of each feature based on KNN model. Assumes table containes X, Y, and Z data.
	# Not working because of python environment issues. <<-------------------------------------------------------------------------### TODO
	def addResiduals( self, table ):
		# First we get the OIDs, and the XYZ Data
		fields = ( 'OBJECTID', 'x', 'y', 'z' )
		sCur = arcpy.da.SearchCursor( table, fields, "", "", "", ( "", "" ) )
		feats = list()
		for row in sCur:
			feats.append( ( row[fields.index( 'OBJECTID' )], row[fields.index( 'x' )], row[fields.index( 'y' )], row[fields.index( 'z' )] ) )
		# feats now contains all of the features from the shapefile
		# Now we simply calculate the residual for each feature
		KNNM = KNNModel( feats, fields.index( 'x' ), fields.index( 'y' ), fields.index( 'z' )  )

	# Projects the passed dataset to the Alaska Albers coordinate system. 
	# Newly projected shapefile replaces the old one.
	# @param table = filepath to the feature class to be projected
	# @param dataset = The dataset the projected data will be copied into.
	# This method is a mess. So much hacking away to try and deal with dataset and workspaces. GAH
	def projectToAA( self, table, dataset=None, transformation=NAD1983_TO_AkAlb_Transformation ):
		if dataset == None:
			dataset = self.WRKSPC
		print( dataset )
		self.printIfVerbose( "Projecting %s to AA." % table )
		# The data needs to be projected into a new dataset, one which is defined as having the AA projection
		# Using the dataset parameter, we can control where the projected data lands.
		( dir, basename ) = os.path.split( table )
		dir = os.path.join( self.GDB, dataset ) 
		out_fc = os.path.join( dir, basename )
		print( out_fc )
		data_type = 'Shapefile'
		# This is code exported by ArcGIS. The long string parameters in it are correct.
		print( "arcpy.Project_management( \"%s\", \"%s\", \"%s\", \"\", \"\", \"NO_PRESERVE_SHAPE\", "", \"NO_VERTICAL\")" % ( table, dataset, "TRAN_STR" ) )
		arcpy.Project_management( table, dataset, transformation, "", "", "NO_PRESERVE_SHAPE", "", "NO_VERTICAL")
		self.updateTableProcessingRecord( table, ['is_proj',], [1,] )
	
	# Builds point geometry for each feature of the passed dataset. Assumes that X and Y fields are already present in the feature class
	def buildGeometry( self, table ):
		# First, we add the shape field
		arcpy.AddField_management( table, 'Shape', 'Geometry' )
		update_fields = ( 'x', 'y', 'Shape' )
		x_index = update_fields.index( 'x' )
		y_index = update_fields.index( 'y' )
		shape_index = update_fields.index( 'Shape' )
		# Then we build the geometry values into the new field
		uCur = arcpy.da.UpdateCursor( table, update_fields )
		for row in uCur:
			point = arcpy.Point( row[x_index], row[y_index] )
			pt_geom = arcpy.PointGeometry( point )
			row[shape_index] = pt_geom
			uCur.updateRow( row )
		del uCur
		del row
	
	def standardizeFieldNames( self, table ):
		# For the sake of consistancy, we need to rename the fields to standard names
		# Due to the large number of different sources of the material, we encounter tables with a wide variety of field names. We want them to all condense into the three we like: x, y, and z
		# In this dictionary, we can define any interesting field names we come across, and when encountered they can be renamed appropriately.
		fields = [f.name for f in arcpy.ListFields( table )]
		rename_dict = { 'POINT_X'	:'x', 
						'POINT_Y'	:'y', 
						'POINT_Z'	:'z', 
						'gridcode'	:'z', 
						'grid_code'	:'z' }
		
		for field in fields:
			if field in rename_dict:
				self.printIfVerbose( "Renaming %s to %s in table %s" % ( field, rename_dict[field], table ) )
				arcpy.AlterField_management( table, field, rename_dict[field] )
	
	# Adds XY coordinates to a table with a geometry field
	def addXYZData( self, table ):
		self.printIfVerbose( "Adding XYZ data to %s." % table )
		arcpy.AddXY_management( table )
		# For the sake of consistancy, we need to rename the fields to standard names
		# Due to the large number of different sources of the material, we encounter tables with a wide variety of field names. We want them to all condense into the three we like: x, y, and z
		# In this dictionary, we can define any interesting field names we come across, and when encountered they can be renamed appropriately.
		fields = [f.name for f in arcpy.ListFields( table )]
		self.standardizeFieldNames( table )
				
		update_fields = ['has_xyz']
		update_values = [1,]
		self.updateTableProcessingRecord( table, update_fields, update_values )
	
	# This function will return an instance of the processing dictionary, which will dictate which processes are applied to which datasets.
	# Each key in the dictionary will refer to a field in the Table Processing Record, and each value will consist of one or more functions.
	# For each dictionary entry, the value functions will be applied to all tables which do not have a 1 in the key field (denoting completed processing)
	# This method streamlines adding processing functionality. In order to add a new layer of processing to the control flow, simply write the processing function (func) which refers to a TPR field (f), and create a new entry in the processing dictionary ( f:func )
	def defineProcessingDictionary( self ):
		proc_dict = {
		'tbl_size'		 :self.calculateTableSize,
		'has_x'          :self.addXYZData,
		'perc'			 :self.addPercentiles,
		'is_proj'        :self.projectToAA,
		'tbl_std_dev'    :self.calculateTableStatistics,
		'has_shp'		 :self.buildGeometry
		}
		return proc_dict
		
	# The main processing method. Cycles through every key in the proc_dict, selects all tables from the tpr with 0's in the key field, then applies the value process to each table.
	# Supports multiprocssing
	def processTables( self ):
		# An inelegant way of ordering the processes. Totally breaks the intended functionality. Needs to be fixed
		# This could be substitued by a single int/char preceding each function. The int/char would indicate the order of the list, and would simply be removed fromt the field_name prior to use. Not as pretty, but better than this list.
		processes = ( 'has_x', 'perc', 'tbl_std_dev', 'has_shp', 'is_proj' )
		for field in processes:
			tables = self.selectTablesByProcessingRecord( field )
			if self.multiprocessing_on:
				# If execution reaches this line, multiprocessing has been turned on
				# We define a work pool, which will map our inputs the the process we speciy, while keeping the number of spawned processes below a set maximum (self.max_num_cpu)
				p = mp.Pool( self.max_num_cpu )
				p.map( self.proc_dict[field], tables )
			else:
				for table in tables:
					try:
						self.proc_dict[field]( table )
					except Exception as e:
						log = open( self.err_log_fp, 'a' )
						log.write( "\nError while processing %s.\nProccessing field: %s\n%s\n" % ( table, field, str( e ) ) )
						log.close()

		
def returnTestProcessor():
	TPR = r"Table_Processing_Record"
	FGDB = r"C:\Users\tristan.sebens\Documents\TerrainTest.gdb"
	return ArcGDBDataProcessor( FGDB, TPR, verbose=True )
	
		
def test():
	TBL = r"ShapefileTest"
	GDB = r"C:\Users\tristan.sebens\Documents\TerrainTest.gdb"
	AGDBP = ArcGDBDataProcessor( True, TPR, GDB )
	ArcGDBDataProcessor.calculateStdDev( AGDBP, TBL )

if __name__ == '__main__':
	test()
		