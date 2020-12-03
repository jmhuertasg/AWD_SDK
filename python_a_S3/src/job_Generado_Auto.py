import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [TempDir, JOB_NAME]
args = getResolvedOptions(sys.argv, ['TempDir','JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)


# Carga datos en el Dynamic_frame de las tablas de atencion_medica_csv y centros_mayores_csv
## @type: DataSource
## @args: [database = "csvfromdatas3", table_name = "atencion_medica_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource0 = glueContext.create_dynamic_frame.from_catalog(database = "csvfromdatas3", table_name = "atencion_medica_csv", transformation_ctx = "datasource0")

## @type: DataSource
## @args: [database = "csvfromdatas3", table_name = "centros_mayores_csv", transformation_ctx = "datasource0"]
## @return: datasource0
## @inputs: []
datasource1 = glueContext.create_dynamic_frame.from_catalog(database = "csvfromdatas3", table_name = "centros_mayores_csv", transformation_ctx = "datasource0")

# Mapeamos los campos de origen de cada tabla con los del destino
## @type: ApplyMapping
## @args: [mapping = [("pk", "string", "pk", "string"), ("nombre", "string", "nombre", "string"), ("descripcion_entidad", "string", "descripcion_entidad", "string"), ("horario", "string", "horario", "string"), ("equipamiento", "string", "equipamiento", "string"), ("transporte", "string", "transporte", "string"), ("descripcion", "string", "descripcion", "string"), ("accesibilidad", "string", "accesibilidad", "string"), ("content_url", "string", "content_url", "string"), ("nombre_via", "string", "nombre_via", "string"), ("clase_vial", "string", "clase_vial", "string"), ("tipo_num", "string", "tipo_num", "string"), ("num", "string", "num", "string"), ("planta", "string", "planta", "string"), ("puerta", "string", "puerta", "string"), ("escaleras", "string", "escaleras", "string"), ("orientacion", "string", "orientacion", "string"), ("localidad", "string", "localidad", "string"), ("provincia", "string", "provincia", "string"), ("codigo_postal", "string", "codigo_postal", "string"), ("barrio", "string", "barrio", "string"), ("distrito", "string", "distrito", "string"), ("coordenada_x", "string", "coordenada_x", "string"), ("coordenada_y", "string", "coordenada_y", "string"), ("latitud", "string", "latitud", "string"), ("longitud", "string", "longitud", "string"), ("telefono", "string", "telefono", "string"), ("fax", "string", "fax", "string"), ("email", "string", "email", "string"), ("tipo", "string", "tipo", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource0]
applymapping0= ApplyMapping.apply(frame = datasource0, mappings = [("pk", "string", "pk", "string"), ("nombre", "string", "nombre", "string"), ("descripcion_entidad", "string", "descripcion_entidad", "string"), ("horario", "string", "horario", "string"), ("equipamiento", "string", "equipamiento", "string"), ("transporte", "string", "transporte", "string"), ("descripcion", "string", "descripcion", "string"), ("accesibilidad", "string", "accesibilidad", "string"), ("content_url", "string", "content_url", "string"), ("nombre_via", "string", "nombre_via", "string"), ("clase_vial", "string", "clase_vial", "string"), ("tipo_num", "string", "tipo_num", "string"), ("num", "string", "num", "string"), ("planta", "string", "planta", "string"), ("puerta", "string", "puerta", "string"), ("escaleras", "string", "escaleras", "string"), ("orientacion", "string", "orientacion", "string"), ("localidad", "string", "localidad", "string"), ("provincia", "string", "provincia", "string"), ("codigo_postal", "string", "codigo_postal", "string"), ("barrio", "string", "barrio", "string"), ("distrito", "string", "distrito", "string"), ("coordenada_x", "string", "coordenada_x", "string"), ("coordenada_y", "string", "coordenada_y", "string"), ("latitud", "string", "latitud", "string"), ("longitud", "string", "longitud", "string"), ("telefono", "string", "telefono", "string"), ("fax", "string", "fax", "string"), ("email", "string", "email", "string"), ("tipo", "string", "tipo", "string")], transformation_ctx = "applymapping1")

## @type: ApplyMapping
## @args: [mapping = [("pk", "string", "pk", "string"), ("nombre", "string", "nombre", "string"), ("descripcion_entidad", "string", "descripcion_entidad", "string"), ("horario", "string", "horario", "string"), ("equipamiento", "string", "equipamiento", "string"), ("transporte", "string", "transporte", "string"), ("descripcion", "string", "descripcion", "string"), ("accesibilidad", "string", "accesibilidad", "string"), ("content_url", "string", "content_url", "string"), ("nombre_via", "string", "nombre_via", "string"), ("clase_vial", "string", "clase_vial", "string"), ("tipo_num", "string", "tipo_num", "string"), ("num", "string", "num", "string"), ("planta", "string", "planta", "string"), ("puerta", "string", "puerta", "string"), ("escaleras", "string", "escaleras", "string"), ("orientacion", "string", "orientacion", "string"), ("localidad", "string", "localidad", "string"), ("provincia", "string", "provincia", "string"), ("codigo_postal", "string", "codigo_postal", "string"), ("barrio", "string", "barrio", "string"), ("distrito", "string", "distrito", "string"), ("coordenada_x", "string", "coordenada_x", "string"), ("coordenada_y", "string", "coordenada_y", "string"), ("latitud", "string", "latitud", "string"), ("longitud", "string", "longitud", "string"), ("telefono", "string", "telefono", "string"), ("fax", "string", "fax", "string"), ("email", "string", "email", "string"), ("tipo", "string", "tipo", "string")], transformation_ctx = "applymapping1"]
## @return: applymapping1
## @inputs: [frame = datasource1]
applymapping1 = ApplyMapping.apply(frame = datasource1, mappings = [("pk", "string", "pk", "string"), ("nombre", "string", "nombre", "string"), ("descripcion_entidad", "string", "descripcion_entidad", "string"), ("horario", "string", "horario", "string"), ("equipamiento", "string", "equipamiento", "string"), ("transporte", "string", "transporte", "string"), ("descripcion", "string", "descripcion", "string"), ("accesibilidad", "string", "accesibilidad", "string"), ("content_url", "string", "content_url", "string"), ("nombre_via", "string", "nombre_via", "string"), ("clase_vial", "string", "clase_vial", "string"), ("tipo_num", "string", "tipo_num", "string"), ("num", "string", "num", "string"), ("planta", "string", "planta", "string"), ("puerta", "string", "puerta", "string"), ("escaleras", "string", "escaleras", "string"), ("orientacion", "string", "orientacion", "string"), ("localidad", "string", "localidad", "string"), ("provincia", "string", "provincia", "string"), ("codigo_postal", "string", "codigo_postal", "string"), ("barrio", "string", "barrio", "string"), ("distrito", "string", "distrito", "string"), ("coordenada_x", "string", "coordenada_x", "string"), ("coordenada_y", "string", "coordenada_y", "string"), ("latitud", "string", "latitud", "string"), ("longitud", "string", "longitud", "string"), ("telefono", "string", "telefono", "string"), ("fax", "string", "fax", "string"), ("email", "string", "email", "string"), ("tipo", "string", "tipo", "string")], transformation_ctx = "applymapping1")


## @type: ResolveChoice
## @args: [choice = "make_cols", transformation_ctx = "resolvechoice2"]
## @return: resolvechoice2
## @inputs: [frame = applymapping1]
#resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_cols", transformation_ctx = "resolvechoice2")

#Eliminacion de campos Nulos
## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = applymapping0]
dropnullfields0 = DropNullFields.apply(frame = applymapping0, transformation_ctx = "dropnullfields3")

## @type: DropNullFields
## @args: [transformation_ctx = "dropnullfields3"]
## @return: dropnullfields3
## @inputs: [frame = applymapping1]
dropnullfields1 = DropNullFields.apply(frame = applymapping1, transformation_ctx = "dropnullfields3")

# Inserccion de los registros del frame tratados en el destino , cada uno a su tabla indicada en Redshift
## @type: DataSink
## @args: [catalog_connection = "redshift2", connection_options = {"dbtable": "atencion_medica_csv", "database": "tfmredshiftdata"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink0
## @inputs: [frame = dropnullfields3]
datasink0 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields0, catalog_connection = "redshift2", connection_options = {"dbtable": "atencion_medica_csv", "database": "tfmredshiftdata"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()

## @type: DataSink
## @args: [catalog_connection = "redshift2", connection_options = {"dbtable": "centros_mayores_cvs", "database": "tfmredshiftdata"}, redshift_tmp_dir = TempDir, transformation_ctx = "datasink4"]
## @return: datasink1
## @inputs: [frame = dropnullfields3]
datasink1 = glueContext.write_dynamic_frame.from_jdbc_conf(frame = dropnullfields1, catalog_connection = "redshift2", connection_options = {"dbtable": "centros_mayores_csv", "database": "tfmredshiftdata"}, redshift_tmp_dir = args["TempDir"], transformation_ctx = "datasink4")
job.commit()