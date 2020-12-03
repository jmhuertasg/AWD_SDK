#!/usr/bin/env python
from configparser import ConfigParser
import os
import botocore
import logging
import boto3
from botocore.exceptions import ClientError

URL_CFG    ='C:\\Users\\JMHUERTASG\\.aws\\config.ini'
URL_DATA   ='...\DATOS MADRID PARA EL PROYECTO'
ACCESS_KEY ="*"
SECRET_KEY =""
REGION_KEY ="eu-west-1"
BUCKET_NAME="ftm-datos-gov"

class LoadFielesS3(object):
    #LISTARDIR
    #Obtiene ficheros para subir a S3
    def listardir(URL_DATA):
        return os.listdir(URL_DATA)

    #Comprobar existencia del S3 a utilizar
    def bucket_exists(bucket_name):
        """
        Returns ``True`` if a bucket exists and you have access to
        call ``HeadBucket`` on it, otherwise ``False``.
        """
        try:
            s3 = boto3.resource('s3')
            s3.meta.client.head_bucket(Bucket="tfm_datos_gov")
            print("Si existe")
            return True
        except ClientError:
            print("No existe")
            return False
    #Subida de los ficheros al bucket S3
    def upload_to_s3(self, filepath):
        """
        Upload a file to the S3 input file bucket.
        """
        filename = os.path.basename(filepath)
        with open(filepath, 'rb') as data:
            s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,aws_secret_access_key=SECRET_KEY)
            self.in_bucket.Object(filename).put(Body=data)
        print("Uploaded raw video {0}".format(filename))
        return filename
    #Carga de la configuracion necesaria desde fichero .ini
    def  loadconf( tag, key):
        """
        Lectura del fichero de configuracion AWS/config.ini
        :param tag:
        :param key:
        :return: valor asociado a la key
        """
        parser = ConfigParser()
        parser.read(URL_CFG)
        return parser.get('default_section', key)

    def check_bucket(bucket):
        try:
            s3.meta.client.head_bucket(Bucket=s3_name)
            print("Bucket Exists!")
            return True
        except botocore.exceptions.ClientError as e:
            # If a client error is thrown, then check that it was a 404 error.
            # If it was a 404 error, then the bucket does not exist.
            error_code = int(e.response['Error']['Code'])
            if error_code == 403:
                print("Private Bucket. Forbidden Access!")
                return True
            elif error_code == 404:
                print("Bucket Does Not Exist!")
                return False

    #Subida de los ficheros al bucket S3
    def upload_file(file_name, bucket, object_name=None):
        """Upload a file to an S3 bucket
        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if object_name is None:
            object_name = file_name

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            response = s3_client.upload_file(file_name, bucket, object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    #Obtencion de ficheros ,directorios
    list_fields=LoadFielesS3.listardir(URL_DATA)
    for x in range(0, len(list_fields)):
        print("Obtenemos ficheros para subirlos:"+list_fields[x])

    #Obtenemos las credenciales de AWS
    aws_access_key_id=LoadFielesS3.loadconf('default', 'aws_access_key_id')
    aws_secret_access_key=LoadFielesS3.loadconf('Cliente_Tfm', 'aws_secret_access_key')
    region=LoadFielesS3.loadconf('Cliente_Tfm', 'region')
    s3_name=LoadFielesS3.loadconf('Cliente_Tfm', 's3_name')

    #Comprobar existencia de backet
    s3 = boto3.resource('s3')
    bucket = s3.Bucket(s3_name)
    LoadFielesS3.check_bucket(bucket)

    #Recorre la lista de ficheros que se tiene que subir
    #Subimos cada fichero al bucket
    for x in range(0, len(list_fields)):
      #print(list_fields[x])
      path_full=URL_DATA + '\\' + list_fields[x]
      if (LoadFielesS3.upload_file(path_full, s3_name, list_fields[x])):
          print("Fichero %s subido con exito:" % list_fields[x])
      else :
          print("No subido")