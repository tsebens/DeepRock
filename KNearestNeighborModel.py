# KNN Model
# Author: Tristan Sebens
# Contact: tristan.ng.sebens@gmail.com
# Date created: 8/APR/2015
# Takes in an array of XYZ coordinates (potentially among other data), then calculates expected values for each point based on its' neighboring points
# The difference between this expected value and the observed value is called the residual
from scipy.spatial import KDTree as kd
import sys
import math

# TODO: Optional - Automatically adjust the number of neighbors used in the calculation to attain a desired rate of points processed per second
NUM_NN = 150 # The default number of nearest neighbors to search for
MAXIMUM_RECURSION_DEPTH = 2000 # The default maximum depth of recursion allowed.

# Options for construction of KD tree. Mostly matter based on the system being used (memory, speed, etc.)

# The indexes of the three values within the array
class KNNModel( object ):
	def __init__( self, data, x_index, y_index, z_index, NUM_NN=150, MRD=2000, LS=None ):
		self.data = data
		self.size = len( data )
		# The data array provided to the class to build the KD Tree from can contain data other than XYZ data. Here we define the indices at which these values in particular can be found in each row of the provided data
		self.INDEX_OF_X_VALUES = x_index
		self.INDEX_OF_Y_VALUES = y_index
		self.INDEX_OF_Z_VALUES = z_index
		self.NUM_NN = NUM_NN # The default number of nearest neighbors to search for
		self.MAXIMUM_RECURSION_DEPTH = MRD # The default maximum depth of recursion allowed.
		if LS != None:
			self.LEAF_SIZE = LS
		else:
			self.LEAF_SIZE = float( self.size / ( math.pow( 2.0, math.log2( float( self.MAXIMUM_RECURSION_DEPTH + 1 ) ) - 1 )) + 100 ) # I don't really understand this math very well, but it works. It basically calculates the minimum possible leaf size while keeping the number of recursive calls below the MRD
		if LEAF_SIZE < 1: # The leaves have a minimum size of 1
			LEAF_SIZE = 1
		self.KD = self.CreateKDTree()
			
	def GetNumberOfNearestNeighbors( self ):
		return self.NUM_NN

	def SetNumberOfNearestNeighbors( self, NUM ):
		self.NUM_NN = NUM
		
	def GetMaximumRecursionDepth( self ):
		return self.MAXIMUM_RECURSION_DEPTH
		
	def SetMaximumRecursionDepth( self, MAX ):
		self.MAXIMUM_RECURSION_DEPTH = MAX

	def GetXYdata( self, TABLE ):
		XY = list()
		for ROW in TABLE:
			XY.append( ( ROW[self.INDEX_OF_X_VALUES], ROW[self.INDEX_OF_Y_VALUES] ) )
		return XY
		
	def GetXYZdata( self, TABLE ):
		XYZ = list()
		for ROW in TABLE:
			XYZ.append( ( ROW[self.INDEX_OF_X_VALUES], ROW[self.INDEX_OF_Y_VALUES], ROW[self.INDEX_OF_Z_VALUES] ) )
		return XYZ

	def GetZValueAt( self, TABLE, INDEX ):
		return TABLE[INDEX][self.INDEX_OF_Z_VALUES]

	def GetXValueAt( self, TABLE, INDEX ):
		return TABLE[INDEX][self.INDEX_OF_X_VALUES]
		
	def GetYValueAt( self, TABLE, INDEX ):
		return TABLE[INDEX][self.INDEX_OF_Y_VALUES]

	# Retrieve the indexes of those points in TABLE closest to the point at index INDEX in TABLE
	# @param INDEX = The index of the point in questions
	# @param KD = The KD Tree used to find the nearest neighbors. Should already be populated
	# @param TABLE = The table of values from which we are finding our nearest neighbors
	# @param Distances = Default is False. If manually set to True, function returns the 
	# distances to the nearest neighbors, not the indexes
	def GetKNNIndexes( self, INDEX, KD, TABLE, Distances=False ):
		# Retrieve the indexes of the K (NUM_NN) nearest neighbors
		result = KD.query( ( self.GetXValueAt( TABLE, INDEX ), self.GetYValueAt( TABLE, INDEX ) ), k=self.NUM_NN )
		if Distances == False:
			# The return of KD.query() is actually a list of lists, with the first list being a list of distances, 
			# and the second being a list of indexes. We only want the indexes, so we grab the list at index 1
			return result[1]
		
		# If we make it here, it means that for some reason, we want the distances instead of the indexes
		# I just put this here in case I needed it later
		return result[0]
		
	# Return the average z value of the K (NUM_NN) nearest neighbors
	# @param INDEX = The index of the point in question in TABLE
	# @param table = The table of values
	def GetAvgNN( self, INDEX, TABLE ):
		# Retrieve the indexes of the K (NUM_NN) nearest neighbors
		NN = GetKNNIndexes( INDEX, self.KD, TABLE )
		# Sum the Z values of the nearest neighbors
		sum = 0
		for INDEX in NN:
			sum += GetZValueAt( TABLE, INDEX )
		# Return the average Z value
		return sum / len( NN )

	# Creates and populates a KD Tree from the XY values of the passed data
	# Assumes that the table's x and y values will be in the predicted indexes (0 and 1, respectively)
	def CreateKDTree( self ):
		# Here we increase the recursive limit. By both increasing the recursive limit, and defining our maximum leaf size 
		# based on the recursive limit and the size of the data set, we can improve our efficiency
		old_limit = sys.getrecursionlimit()
		sys.setrecursionlimit( self.MAXIMUM_RECURSION_DEPTH )
		XY = self.GetXYdata( self.data )
		KD = kd( XY, LEAF_SIZE )
		sys.setrecursionlimit( old_limit ) # Here we reset the recursive limit back to it's old, not-awesome value
		return KD
		
	# Calculates the residual of the point at INDEX in TABLE
	# @param INDEX = The index in TABLE of the point in question 
	# @param TABLE = The table of all values
	def CalculateResidual( self, INDEX, TABLE ):
		AVG_NN = self.GetAvgNN( INDEX, TABLE )
		# Residual = OBSERVED - EXPECTED ( This way, values higher than expected will have positive residuals, and vice versa
		RESIDUAL = self.GetZValueAt( TABLE, INDEX ) - AVG_NN
		return RESIDUAL

	# Calculates the residuals of all points contained in the passed table
	# @param TABLE = A nx3 array of n xyz coordinates.
	# @return = An nx4 array. Each of the n rows contains the original xyz values, plus the calculated residual for that data point.
	def CalculateAllResiduals( self, TABLE ):
		KD = CreateKDTree( TABLE )
		INDEX = 0
		while INDEX < len( TABLE ):
			RESIDUALS.append( CalculateResidual( INDEX, KD, TABLE ) )
			INDEX += 1
		return RESIDUALS