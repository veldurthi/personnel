# Databricks notebook source
import glob,os,time
from asammdf import MDF, MDF4, Signal
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql
from pyspark.sql import Row
import io
from delta.tables import *
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from pyspark.sql.types import StructField
from pyspark.sql.types import StructType
import datetime

# COMMAND ----------

t1 = pa.int64()
t1

# COMMAND ----------

pandasDfList = list()
mf4_VIN="VIN"
mf4_RINGBUFFER = "RINGBUFFER"
mf4_FILEINDEX = "FILEINDEX"
mf4_TIMESTAMP ="TIMESTAMP"
selectedSignals = ["Accel_X_Cval",
"Accel_Y_Cval",
"AccelPdlPosn_Cval",
"BrkPdlPosn_Cval",
"OilTemp_RA2_Cval",
"EngCoolTemp_Cval",
"EngOilTemp_Cval",
"StwAngl_Cval",
"VehSpd_Cval_CPC",
"YawRate_Cval_BS",
"CC_Actv_Stat",
"CC_SetSpd_Cval",
"EngTrq_Cval_PT",
"OilTemp_RA_Cval",
"SpdR_AxleLtWhl_Cval",
"SpdR_AxleRtWhl_Cval",
"BrkAirPress1_Stat_ICUC",
"BrkAirPress2_Stat_ICUC",
"DateTmHour_Cval_ICUC",
"DateTmMinute_Cval_ICUC",
"DateTmSecond_Cval_ICUC",
"LocalHourOffset_Cval_ICUC"]

# COMMAND ----------

"""
Get the data type of a raw signal object from ASAMMDF library
Depending on available attributes (factor, number of bits) conversion block of mf4 file
Further mf4 conversion information: "https://www.asam.net/standards/detail/mdf/wiki/"
"""
def getSignalDtypeTerrel(rawSignals):
  numericDtypes = dict()
  print("=============================================================================================")
  #0: int32, 1: int64, 2: float64, 3: string
  #dtypeSignals = [[] for i in range(4)]   
  for s in rawSignals:
    if(s.name in (selectedSignals)):  
      print(s.name)
      print("dtype is ")
      print(s.samples.dtype)
      conv = s.conversion
#     print("conversion print")
#     print(conv)
      if(conv is not None): 
#       print(1)
        ctype = s.conversion.conversion_type
        print("Ctype is")
        print(ctype)
        nr = s.conversion.val_param_nr
#       print(nr)
        if ctype == 1:
            numericDtypes[s.name] = getDtypeFromConversion(s.samples.dtype, conv.a), True
        elif ctype == 7:
          try:
            default = s.conversion.referenced_blocks.get("default_addr", {})
          # default.a is factor: (default.b is offset - can be ignored as it doesnt change data type)
            numericDtypes[s.name] = getDtypeFromConversion(s.samples.dtype, default.a), True
          except:
            numericDtypes[s.name] = getDtypeFromConversion(s.samples.dtype), False
            continue
        else:
        # 1:1 conversion
          dtype = getDtypeFromConversion(s.samples.dtype)
          if dtype:
          # prevent adding string signals like Marker with empty dtype
            numericDtypes[s.name] = dtype, False
      else:
      # signals without conversion 
        numericDtypes[s.name] = getDtypeFromConversion(s.samples.dtype), False
         
  return numericDtypes

"""
Get dtype for a signal depending on raw signal samples dtype and conversion factor
Possible datatypes: Integer, Double, Float, String
"""
def getDtypeFromConversion(sampleDtype, factor = None):
  bits = sampleDtype.itemsize * 8
  kind = sampleDtype.kind
  hasFactor = factor is not None
  isUnsigned = kind == 'u'
  isSigned = kind == 'i'
  isFloat = kind == 'f'
  isString = kind == 'S'
  wholeFactor = None 
  
  if (hasFactor):
    wholeFactor = factor.is_integer()
  
  if (not hasFactor or wholeFactor) and not isFloat:
    if (isUnsigned and bits < 32) or (isSigned and bits <= 32):
      return pa.int32()
    elif (isUnsigned and bits >= 32) or (isSigned and bits <= 32):
      return pa.int64()
    elif isString:
      return pa.string()

  elif (not wholeFactor or isFloat):
    return pa.float64()
  
"""
Get a unique filename with Timestamp of mf4 + Filename for filename col 
"""
def getUniqueMf4FileName(mdf, file_path):
  start_time = mdf.header.start_time
  vin = getCarName(mdf)
#   return vin + " " + str(start_time) + " " + os.path.basename(file_path)
  return os.path.basename(file_path)


"""
Read car name from mf4 header comment
"""
def getCarName(mdf):
  mdfHeaderComment = mdf.header.comment
  carTag = "Car: ["
  carTagLength = len(carTag)
  posCar = mdfHeaderComment.find(carTag) + carTagLength
  posCarEnd = mdfHeaderComment.find("]", posCar)
  return mdfHeaderComment[posCar:posCarEnd]

# COMMAND ----------

"""
Read mf4 file (mdf object from OpenSource ASAMMDF library)
For each signal
- get signal data type
- get physical values (time series data)
- add data to panda dataframe
- build schema for following pandas dataframe to spark conversion
"""
def mf4ToPandasDf(vehicle_id,mdf, filePath, partitionList):
  global mf4_parser_time_col, mf4_parser_filename_col, mf4_VIN, mf4_RINGBUFFER, mf4_FILEINDEX, mf4_TIMESTAMP
  signalList = list(mdf.channels_db.keys())
  signals = mdf.select(signalList, ignore_value2text_conversions = True, raw = True)
  # get data type from raw data 
  columnDtypes = getSignalDtype(signals)
  masters = [mdf.get_master(i) for i, _ in enumerate(mdf.groups)]
  masters = [master for master in masters if len(master)]
  if masters:
    master = np.unique(np.concatenate(masters))
  else:
    master = np.array([], dtype='float64')
    
  df = pd.DataFrame()  
  df["timestamps"] = pd.Series(master, index=np.arange(len(master)))
  df.set_index('timestamps', inplace=True)
#   mf4StartTime = mdf.header.start_time  
  # vechicle_id
  #   filePath="/dbfs/mnt/westus2_development/ADX_TEMP/MF4_Sample/E92845_Data1_F088_2020-04-30_18-56-01.mf4"
  createdFileName = getUniqueMf4FileName(mdf, filePath)
# Added for new columns
  splitfileName=createdFileName[0:-4].split("_")
  VIN=splitfileName[0]
  RINGBUFFER=splitfileName[1]
  FILEINDEX=splitfileName[2]
# TIMESTAMP=splitfileName[3]+" "+splitfileName[4]
  mf4StartTime = mdf.header.start_time 
  absoluteTime=mf4StartTime.timestamp()
  TIMESTAMP = pd.to_datetime(absoluteTime, unit='s')
  print(absoluteTime)
#   df["vehicle_id"] = vehicle_id
#   // ended for nw columns
  df[mf4_VIN] = VIN
  df[mf4_RINGBUFFER] = RINGBUFFER
  df[mf4_FILEINDEX] = FILEINDEX
  df[mf4_TIMESTAMP] = TIMESTAMP
#   df[mf4_parser_filename_col] = createdFileName
    # Absolute Time 
#   absoluteTime = np.array(df.index) + mf4StartTime.timestamp()
#   absoluteTime = pd.to_datetime(absoluteTime, unit='s')
#   df[mf4_parser_time_col] = pd.Series(absoluteTime, index=df.index).astype('datetime64[ms]')
    # Schema for Spark dataframe
  structTypes = list()
#   structTypes.append(pa.field("vehicle_id",pa.string(),False))
  structTypes.append(pa.field(mf4_VIN,pa.string(),False))
  structTypes.append(pa.field(mf4_RINGBUFFER,pa.string(),False))
  structTypes.append(pa.field(mf4_FILEINDEX,pa.string(),False))
  structTypes.append(pa.field(mf4_TIMESTAMP,pa.timestamp(unit='ms'),False))
#   structTypes.append(pa.field(mf4_parser_filename_col, pa.string(), False))
#   structTypes.append(pa.field(mf4_parser_time_col, pa.timestamp(unit='ms'), False))
  
  for signal in signals:
    name = signal.name
#     print("Signal Name - "+name)
#     s = signal.validate()
    samples = signal.samples    
    # only add signal as column if valid data exists 
    if(len(samples) > 0):
#       print("A")
      timestamps = signal.timestamps      
      colDtype, hasConversion = columnDtypes[name]      
      # handling exceptional case change scenario
    if hasConversion:
#       print("B")
        # Get converted signal if not all values are strings
      samples = signal.physical().samples 
      # Remove duplicates - happens for system (vehicle logger internal) channels
    pds = pd.Series(samples, timestamps)
#     print(pds)
    pds = pds[~pds.index.duplicated()]
#     print(pds)      
      # Add signal to dataframe
    dtype = samples.dtype
    if dtype.kind == 'S':
#         print("1")
        pds = pds.astype(str).str.decode('utf-8')
        # byte string
        nameStr = name
        df[nameStr] = pds        
        if hasConversion == True:
#           print("2")
          # Signal has Tabular string and numeric value in mf4
          structTypes.append(pa.field(nameStr,pa.string(),True))
          pds = pd.to_numeric(pds, errors='coerce')
          if not np.isnan(pds).all():
#             print("3")
            # if numeric values are in series add to signal name column -> contains only numeric 
            structTypes.append(pa.field(name,colDtype,True))
            df[name] = pds
        else:
          # Signal has only string in mf4
#           print("4")
          structTypes.append(pa.field(nameStr, colDtype, True))      
          
    else:
        # numeric
#         print("5")
        df[name] = pds  
        structTypes.append(pa.field(name, colDtype, True))
  schema = pa.schema(structTypes)
  return (df, schema , createdFileName)

# COMMAND ----------

"""
Main bronze staging function
Call function to parse a single mf4 file
Write file to vehicle delta table 
"""
def bronzeStageMf4(vehicle_id,file,filePath,\
                destinationFolder,\
                version = "001",\
                parsingErrors = dict()):
  
  try:   
    byte_stream = io.BytesIO(file)
    mdf = MDF(byte_stream, version='4.10')
    pdf,schema,createdFileName = mf4ToPandasDf(vehicle_id,mdf, filePath, partitionList = ["Year", "Month"]) 
    if pdf is not None:
      new_destination_filename = destinationFolder + createdFileName.replace(" ", "_").replace(":","_")[0:-4] + ".parquet"     
      table = pa.Table.from_pandas(pdf,schema=schema)
      pq.write_table(table,new_destination_filename)

  except Exception as e:
    raise Exception("Exception while parsing , " + filePath + "\n")

# COMMAND ----------

# MAGIC %fs ls dbfs:/mnt/development/ADX_TEMP/MF4_Sample/

# COMMAND ----------

directoryPath = "/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/*.mf4"
numOfPartitions = len(glob.glob(directoryPath))
print(numOfPartitions)
if(numOfPartitions!=0):
  binaryFilesPath = "dbfs:" + "/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/*.mf4"
  print(binaryFilesPath)
  readBinaryFilesRdd = sc.binaryFiles(path=binaryFilesPath,minPartitions = (numOfPartitions*3))
  outputParquetFolder = "/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_parquet/pysparkfiles/"
  readBinaryFilesRdd.foreach(lambda r:bronzeStageMf4(r[0].split("/")[5].split(".")[0].split("_")[0],r[1],r[0],outputParquetFolder))


# COMMAND ----------

pardf=spark.read.parquet("dbfs:/mnt/development/ADX_TEMP/mf4_parquet/*")
len(pardf.columns)

# COMMAND ----------

display(pardf)

# COMMAND ----------

pardf=spark.read.option("mergeSchema", "true").parquet("dbfs:/mnt/development/ADX_TEMP/MF4_TEST/signal-based_parquet/pysparkfiles/*")

# COMMAND ----------

pardf1=spark.read.parquet("dbfs:/mnt/development/ADX_TEMP/MF4_TEST/signal-based_parquet/python-files/*")
len(pardf1.columns)
pardf2=spark.read.parquet("dbfs:/mnt/development/ADX_TEMP/mf4_parquet/E92845_Data1_F007_2020-04-30_16-36-05.parquet")
lst1=pardf1.columns
lst2=pardf2.columns
print(str(len(lst1)) +" "+str(len(lst2)))
import numpy as np
main_list = np.setdiff1d(lst1,lst2)
print(main_list)

# COMMAND ----------

display(pardf)

# COMMAND ----------

mdf = MDF("/dbfs/mnt/development/ADX_TEMP/MF4_Sample/E92845_Data1_F001_2020-04-30_16-29-21.mf4")
current_df = mdf.to_dataframe()
signalList = list(mdf.channels_db.keys())
len(signalList)

# COMMAND ----------



# COMMAND ----------

mdf = MDF("/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/E94000_Data1_F005_2020-06-26_15-15-53.mf4")
# mdf = MDF("/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/E94000_Data1_F002_2020-06-26_15-00-10.mf4")
current_df = mdf.to_dataframe()
signalList = list(mdf.channels_db.keys())
# signalList

# COMMAND ----------

implist = ["Accel_X_Cval",
"Accel_Y_Cval",
"AccelPdlPosn_Cval",
"BrkPdlPosn_Cval",
"OilTemp_RA2_Cval",
"EngCoolTemp_Cval",
"EngOilTemp_Cval",
"StwAngl_Cval",
"VehSpd_Cval_CPC",
"YawRate_Cval_BS",
"CC_Actv_Stat",
"Altitude_Cval_IPPC",
"CC_SetSpd_Cval",
"Latitude_Cval_IPPC",
"Longitude_Cval_IPPC",
"CurrGr_Stat_PT",
"EngSpd_Cval_PT",
"EngTrq_Cval_PT",
"OilTemp_RA_Cval",
"SpdR_AxleLtWhl_Cval",
"SpdR_AxleRtWhl_Cval",
"SrvBrkSw_Stat_PT",
"TxOilTemp_Cval_TCM",
"XBR_Stat_PT",
"core_m_PccCrestActUcReq_u8",
"core_m_PccDipActUcReq_u8",
"core_m_PccHillActUcReq_u8",
"BrkAirPress1_Stat_ICUC",
"BrkAirPress2_Stat_ICUC",
"IPPC_Stat",
"MapRoadGrad_Cval",
"DateTmHour_Cval_ICUC",
"DateTmMinute_Cval_ICUC",
"DateTmSecond_Cval_ICUC",
"LocalHourOffset_Cval_ICUC"]

len(implist)

# COMMAND ----------

nonexist_list = [item for item in implist if item not in signalList]
exist_list = [item for item in implist if item in signalList]

print(len(nonexist_list))
print(len(exist_list))
nonexist_list

# COMMAND ----------

# MAGIC %md 2664 --05
# MAGIC 
# MAGIC 2484 --001

# COMMAND ----------

signals = mdf.select(signalList, ignore_value2text_conversions = True, raw = True)
columnDtypes = getSignalDtypeTerrel(signals)
# columnDtypes
# df = pd.DataFrame()
# structTypes = list()

# COMMAND ----------

columnDtypes

# COMMAND ----------

for key in sorted(columnDtypes):
    print("%s : %s" % (key, columnDtypes[key]))

# COMMAND ----------

mdf1 = MDF("/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/E94000_Data1_F001_2020-06-26_14-56-34.mf4")
mdf2 = MDF("/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/E94000_Data1_F005_2020-06-26_15-15-53.mf4")
current_df1 = mdf1.to_dataframe()
signalList1 = list(mdf1.channels_db.keys())

current_df2 = mdf2.to_dataframe()
signalList2 = list(mdf2.channels_db.keys())


import numpy as np
main_list = np.setdiff1d(signalList2,signalList1)
print(main_list) 


# COMMAND ----------

for filepath in glob.iglob('/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_mf4/*.mf4'):
  
  inputpath=filepath
#   inputpath="/dbfs/mnt/westus2_development/ADX_TEMP/MF4_Sample/E92845_Data1_F087_2020-04-30_18-54-17.mf4"
  print(inputpath)
  filename=os.path.basename(inputpath)
  print("+++++++++++++++++ starting - " +filename)
  outputpath="/dbfs/mnt/development/ADX_TEMP/MF4_TEST/signal-based_parquet/python-files/"+filename[0:-4] + ".parquet" 
  print(outputpath)
  mdf = MDF(inputpath)
  current_df = mdf.to_dataframe()
  signalList = list(mdf.channels_db.keys())
  signals = mdf.select(signalList, ignore_value2text_conversions = True, raw = True)
  columnDtypes = getSignalDtype(signals)
  df = pd.DataFrame()
  structTypes = list()
#   len(df.columns)

  for signal in signals:
    name = signal.name
#     print("Signal Name - "+name)
#     s = signal.validate()
    samples = signal.samples    
    # only add signal as column if valid data exists 
    if(len(samples) > 0):
#       print("A")
      timestamps = signal.timestamps      
      colDtype, hasConversion = columnDtypes[name]
      # handling exceptional case change scenario
    if hasConversion:
#       print("B")
        # Get converted signal if not all values are strings
      samples = signal.physical().samples 
      # Remove duplicates - happens for system (vehicle logger internal) channels
    pds = pd.Series(samples, timestamps)
#     print(pds)
    pds = pds[~pds.index.duplicated()]
#     print(pds)      
      # Add signal to dataframe
    dtype = samples.dtype
#     print("colDtype - "+str(colDtype))
    if dtype.kind == 'S':
#         print("1")
        pds = pds.astype(str).str.decode('utf-8')
        # byte string
        nameStr = name
        df[nameStr] = pds        
        if hasConversion == True:
#           print("2")
          # Signal has Tabular string and numeric value in mf4
          structTypes.append(pa.field(nameStr,pa.string(),True))
          pds = pd.to_numeric(pds, errors='coerce')
          if not np.isnan(pds).all():
#             print("3")
            # if numeric values are in series add to signal name column -> contains only numeric 
            structTypes.append(pa.field(name,colDtype,True))
            df[name] = pds
        else:
          # Signal has only string in mf4
#           print("4")
          structTypes.append(pa.field(nameStr, colDtype, True))      
          
    else:
        # numeric
#         print("5")
        df[name] = pds  
        structTypes.append(pa.field(name, colDtype, True))
  schema = pa.schema(structTypes)
  table = pa.Table.from_pandas(df,schema=schema)
  pq.write_table(table,outputpath)
  print("++++++++++++++completed - " +filename)
