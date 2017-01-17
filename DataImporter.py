import os
import os.path
import arcpy
import time
import pathos.multiprocessing as mp

class DataImporter( object ):
	def __init__( self, verbose ):
		self.verbose = verbose
		self.procFuncDict = self.defImportFunctionDictionary()
		
	def __str__( self ):
		print( self )
	
	def printIfVerbose( self, message ):
		if self.verbose:
			print( message )
			
	def importFilesFromDir( self, DIR ):
		self.printIfVerbose( "Import files from directory not implemented" )
	
	def importFilesFromList( self, FILES ):
		self.printIfVerbose( "Import files from list not implemented" )
		
	def importShapefile( self, shapefile ):
		self.printIfVerbose( "Import files from shapefile not implemented" )
		
	def importXYZFile( self, xyzfile ):
		self.printIfVerbose( "Import files from xyz file not implemented" )
		
	def importM77tFile( self, m77tfile ):
		self.printIfVerbose( "Import files from m77t file not implemented" )
	
	# Here we define the import function dictionary. Each entry key is a file type, and each entry value is the function with which to import that file type
	# To add import functionality for a new file type (ft), write the import function (func), then add a new dictionary entry { 'ft':func }
	def defImportFunctionDictionary( self ):
		return { '.shp':ArcGDBDataImporter.importShapefile, '.xyz':self.importXYZFile, '.m77t':self.importM77tFile }
		
	
class ArcGDBDataImporter( DataImporter ):
	# @param GDB_fp - The file path to the GDB which this DataImporter will import files into
	# @param verbose - If set to true, this DataImporter
	def __init__( self, GDB_fp, tpr="Table_Processing_Record", dataset=None, verbose=False, multiprocessing_on=False, free_cores=6 ):
		self.GDB = GDB_fp
		self.TPR = os.path.join( self.GDB, tpr )
		self.verbose = verbose
		self.dataset = dataset
		self.multiprocessing_on = multiprocessing_on
		self.max_num_cpu = mp.cpu_count() - free_cores # Given the load imposed by this kind of data processing, it is usually easier to specify how many cores to keep free (free_cores), and then to use all other available cores.
		self.import_dict = self.defImportFunctionDictionary()
		self.WRKSPC = self.GDB
		if dataset != None:
			self.WRKSPC = os.path.join( self.GDB, dataset )
		arcpy.env.workspace = self.WRKSPC
	
	# Method that walks through a file tree recursively, looking for files with the specified extension.
	# It then returns a list containing all files which mached the specified extension
	# @param ROOT = The root of the file tree to search through
	# @param EXTENSION = The extension to look for
	def findFilesByExtension( self, ROOT, EXTENSION ):
		AllFiles = os.walk( ROOT, True, None, False )
		ReturnFiles = list()

		for entry in AllFiles:
			# entry is a tuple containing (path, directories, files)
			for file in entry[2]:
				( filename, extension ) = os.path.splitext( file )
				if extension == EXTENSION:
					ReturnFiles.append( os.path.join( entry[0], file ) )

		return ReturnFiles
	
	def getTableFields( self, table ):
		fields = [f.name for f in arcpy.ListFields( table )]
		return fields
	
	def tablePresentInTPR( self, table ):
		sCur = arcpy.da.SearchCursor( self.TPR, 'tbl_name' )
		for row in sCur:
			if row[0] == table:
				return True
		return False

	# TODO: This function in general needs help << --------------------------------------------------------------------------####
	# @param table - The filename to the new table as inserted into the GDB
	def addTableToTableRecord( self, table ):
		self.printIfVerbose( "Adding %s to table record." % table )
		cursor = arcpy.InsertCursor( self.TPR )
		row = cursor.newRow()
		row.setValue( 'tbl_name', table )
		row.setValue( 'date_added', time.strftime("%Y/%m/%d") )
		cursor.insertRow( row )
		del cursor
		del row

	# Deletes an entry in the TPR based on the table name. Returns true if successful, false if not.
	def removeTableFromTableRecord( self, table ):
		self.printIfVerbose( "Removing %s from table record." % table )
		cursor = arcpy.UpdateCursor( self.TPR )
		for row in cursor:
			if row.getValue( 'tbl_name' ) == table:
				cursor.deleteRow( row )
				del cursor
				del row
				return True
		del cursor
		del row
		return False
		
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
	
		
	# Crawls a directory looking for files and categorizing them by their extension
	# Returns a dictionary with a key for every file type present in the directory, and a list containing all files of that type in the directory as the value for that key
	def categorizeFilesInDir( self, DIR ):
		all_dirs = os.walk( DIR, True, None, False )
		files = {}
		for dir in all_dirs:
			for file in dir[2]:
				( fname, ext ) = os.path.splitext( file )
				if ext in files:
					files[ext] = files[ext] + ( os.path.join( dir[0], file ), )
				else:
					files[ext] = ( os.path.join( dir[0], file ), )
		return files
	
	def deleteEmptyPoints( self, fp ):
		file = open( fp, 'r' )
		xyz = [line.split('\t') for line in file]
		ret = list()
		for point in xyz:
			if point[2] == 'NaN\n':
				pass
			else:
				ret.append( point )
		if len( xyz ) == len( ret ):
			# Culling removed no points, skip the rest
			return
		file.close()
		file = open( fp, 'w' )
		for point in ret:
			file.write( '\t'.join( point ) )
	
	# Imports files of all viable types from the given directory
	# @param skip If True the script will print all errors to the command line, but will then continue. If False will raise error.
	def importFilesFromDir( self, dir ):
		self.printIfVerbose( "Importing files from %s." % dir )
		files = self.categorizeFilesInDir( dir )
		for file_type in files:
			if file_type in self.import_dict:
				# If execution reaches this section, then file_type is a file type we can import, and files[file_type] is a list of files of that file type
				if self.multiprocessing_on:
					# Creates a worker pool which will allow us to execute many processes in parallel, while keeping a cap on how many processes we spawn
					p = mp.ProcessPool( nodes=self.max_num_cpu )
					# A little confusing. self.import_dict[file_type] is the import function to be used to import a file of type file_type. files[file_type] is a list of files of type file_type
					# pool.map spawns worker processes executing the specified import function on each member of files[file_type]
					p.map( self.import_dict[file_type], files[file_type] )
				else:
					# Exactly the same as above, just one at a time. Linear, not parallel.
					for file in files[file_type]:
						self.import_dict[file_type](file)

	def importM77tFile( self, file ):
		( dir, basename ) = os.path.split( file )
		( fname, ext ) = os.path.splitext( basename )
		if self.tablePresentInTPR( fname ):
			self.printIfVerbose( "%s already present in TPR. Cancelling import." % fname )
			return
		self.printIfVerbose( "Importing %s as m77tFile..." % file )
		reader = open( file, 'r' )
		firstline = reader.readline()
		fields = firstline.split( '\t' )
		desired_fields = ( 'LAT', 'LON', 'CORR_DEPTH' )
		desired_field_indices = [fields.index( desired_field ) for desired_field in desired_fields]
		data = list()
		for line in reader:
			row = line.split( '\t' )
			try:
				add_row = [row[index].rstrip() for index in desired_field_indices]
			except:
				continue
			if str( add_row[0] ) == "" or str( add_row[1] ) == "" or str( add_row[2] ) == "" or str( add_row ) == '()':
				continue
			else:
				data.append( add_row )
		reader.close()
		# Data now consists of tuples, each corresponding to a different feature in the m77t file and made up of those fields specified by desired_fields (x,y,z)
		# Now we write these tuples to file as an xyz file, and import the new file
		new_xyz_file_name = os.path.join( dir, fname + '.xyz' )
		new_xyz_file = open( new_xyz_file_name, 'w' )
		for entry in data:
			new_xyz_file.write( '\t'.join( entry ) + '\n' )
		new_xyz_file.close()
		self.importXYZFile( new_xyz_file_name )
	
	# Accepts a file path to the shapefile to be imported, and imports it into the GDB
	def importShapefile( self, file ):
		( fname, ext ) = os.path.splitext( os.path.basename( file ) )
		# Check to see if the file is already present in the TPR.
		if self.tablePresentInTPR( fname ):
			self.printIfVerbose( "%s already present in TPR. Cancelling import." % fname )
			return	
		self.printIfVerbose( "Importing %s as shapefile..." % file )
		arcpy.FeatureClassToGeodatabase_conversion( file, self.WRKSPC ) # Import the fc into the GDB
		self.addTableToTableRecord( fname )
		if self.verbose:
			print( "Done importing %s." % file )

	def importXYZFile( self, file ):
		# fname is the name of the file (w/o extension) and ext is the file extension
		( fname, ext ) = os.path.splitext( os.path.basename( file ) )
		# Check to see if the file is already present in the TPR.
		if self.tablePresentInTPR( fname ):
			self.printIfVerbose( "%s already present in TPR. Cancelling import." % fname )
			return
		if self.verbose:	
			print( "Importing %s as XYZ file..." % file )
			
		self.deleteEmptyPoints( file )
		
		out_fc = os.path.join( self.WRKSPC, fname )
		arcpy.CheckOutExtension("3D")
		arcpy.ddd.ASCII3DToFeatureClass( file, "XYZ", out_fc, "POINT", "1", "GEOGCS['GCS_NAD_1983_2011',DATUM['D_NAD_1983_2011',SPHEROID['GRS_1980',6378137.0,298.257222101]],PRIMEM['Greenwich',0.0],UNIT['Degree',0.0174532925199433]]", "", "", "DECIMAL_POINT")
		arcpy.CheckInExtension("3D")
		self.addTableToTableRecord( fname )
		if self.verbose:
			print( "Done importing %s." % file )			