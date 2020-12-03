package com.ftm.bigdata;

import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.model.PutRecordRequest;
import com.amazonaws.services.kinesisfirehose.model.PutRecordResult;
import com.amazonaws.services.kinesisfirehose.model.Record;


import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Producer_stream_kinesis {
    public static void main(String[] args) throws InterruptedException {
            Producer_stream_kinesis app = new Producer_stream_kinesis();
            app.sendData();
    }

    public void sendData() throws InterruptedException {
         AmazonKinesisFirehose firehoseClient = KinesisFirehoseClient.getFirehoseClient();
         PutRecordRequest putRecordRequest = new PutRecordRequest();
        putRecordRequest.setDeliveryStreamName("Tfm_Kinesis_Streaming");
         putRecordRequest.setRecord(new Record().withData(ByteBuffer.wrap(orderList().getBytes())));
        //send data
        PutRecordResult result = firehoseClient.putRecord(putRecordRequest);
        System.out.println(result.toString());
        Thread.sleep(2000);
    }

    private  DatoStreamKinesis GeneratedRandomData(){
        int index;

        //Creacion del objeto streaming
        DatoStreamKinesis dataStream=new DatoStreamKinesis();

        //obtencion de un valor de la lista de Sexo M o F
        ArrayList<String> sexorandom = new ArrayList<String>();
        sexorandom.add("M");
        sexorandom.add("F");
        index = new java.util.Random().nextInt(sexorandom.size());
        String vSexo = sexorandom.get(index);
        System.out.println("Valor sexo:" +vSexo);
        //obtencion de un valor de la lista de Sexo M o F
    dataStream.setSexo(vSexo);

        //obtencion de un valor de la lista de ciudad
        ArrayList<String> ciudadrandom = new ArrayList<String>();
        ciudadrandom.add("Madrid");
        ciudadrandom.add("Barcelona");
        ciudadrandom.add("Valencia");
        ciudadrandom.add("Sevilla");

        index = new java.util.Random().nextInt(ciudadrandom.size());
        String vCiudad = ciudadrandom.get(index);
        System.out.println("Valor Ciudad:" +vCiudad);
        //obtencion de un valor de la lista de Ciudad
    dataStream.setCiudad(vCiudad);

        //obtencion de un valor de la lista de Poblacion
        ArrayList<String> poblacionrandom = new ArrayList<String>();
        poblacionrandom.add("Madrid");
        poblacionrandom.add("Mostoles");
        poblacionrandom.add("Alcorcon");
        poblacionrandom.add("Getafe");
        poblacionrandom.add("Mataro");
        poblacionrandom.add("Dos Hermanas");


        index = new java.util.Random().nextInt(poblacionrandom.size());
        String vPoblacion = poblacionrandom.get(index);
        System.out.println("Valor Poblacion:" +vPoblacion);
        //obtencion de un valor de la lista de Poblacion
    dataStream.setPoblacion(vPoblacion);

        //obtencion de un valor de la lista de Direccion
        ArrayList<String> direccionrandom = new ArrayList<String>();
        direccionrandom.add("Plaza Mayor");
        direccionrandom.add("Suecia");
        direccionrandom.add("Hermanos Garcia");
        direccionrandom.add("Gran Via");
        direccionrandom.add("Dos de Mayo");
        direccionrandom.add("Rosales");


        index = new java.util.Random().nextInt(direccionrandom.size());
        String vDireccion = direccionrandom.get(index);
        System.out.println("Valor Direccion:" +vDireccion);
        //obtencion de un valor de la lista de vDireccion
    dataStream.setDireccion(vDireccion);

        //obtencion de un valor de la lista de Actividad
        ArrayList<String> actividadrandom = new ArrayList<String>();
        actividadrandom.add("Running");
        actividadrandom.add("Baloncesto");
        actividadrandom.add("Fotbol");
        actividadrandom.add("Judo");
        actividadrandom.add("Natacion");
        actividadrandom.add("Yoga");


        index = new java.util.Random().nextInt(actividadrandom.size());
        String vActividad = actividadrandom.get(index);
        System.out.println("Valor Actividad:" +vActividad);
        //obtencion de un valor de la lista de vActividad
    dataStream.setActividad(vActividad);

        //obtencion de un valor de la lista de Busqueda
        ArrayList<String> busquedarandom = new ArrayList<String>();
        busquedarandom.add("Running");
        busquedarandom.add("Baloncesto");
        busquedarandom.add("Fotbol");
        busquedarandom.add("Judo");
        busquedarandom.add("Natacion");
        busquedarandom.add("Yoga");


        index = new java.util.Random().nextInt(busquedarandom.size());
        String vBusqueda = busquedarandom.get(index);
        System.out.println("Valor Busqueda:" +vBusqueda);
        //obtencion de un valor de la lista de vBusqueda
    dataStream.setBusqueda(vBusqueda);

        //obtencion de un valor random Edad
        int vEdad= (int) (100 * Math.random());
        System.out.println("Valor Edad:" +vEdad);
        //obtencion de un valor rendom Edad
    dataStream.setEdad(vEdad);


        //obtencion de un valor de la lista de email
        ArrayList<String> emailrandom = new ArrayList<String>();
        emailrandom.add("pepepepep@gmail.com");
        emailrandom.add("werawerap@gmail.com");
        emailrandom.add("gggggggep@gmail.com");
        emailrandom.add("ffffffpep@gmail.com");
        emailrandom.add("hhhhhhhep@gmail.com");
        emailrandom.add("bbbbbbpep@gmail.com");
        emailrandom.add("cccccccep@gmail.com");
        emailrandom.add("eeeeeepep@gmail.com");
        emailrandom.add("dffdfdfdf@gmail.com");
        emailrandom.add("hjhjhjhjp@gmail.com");
        emailrandom.add("jkjkjkkep@gmail.com");

        index = new java.util.Random().nextInt(emailrandom.size());
        String vEmail = emailrandom.get(index);
        System.out.println("Valor Email:" +vEmail);
        //obtencion de un valor de la lista de vEmail
    dataStream.setEmail(vEmail);


        int vCp=(int) (getRandomInteger(28999, 28000));
        System.out.println("Valor CP:" +vCp);
        //obtencion de un valor CP entre (28000  y 28999)
    dataStream.setCp(Integer.toString(vCp));

        return dataStream;
    }

    private String orderList(){
       List<DatoStreamKinesis> listStreamKine = new ArrayList<DatoStreamKinesis>();
       StringBuilder sb = new StringBuilder();
       for(int i=0; i<6500; i++) {
          listStreamKine.add(GeneratedRandomData());
       }
       DatoStreamKinesis[] listingArray = listStreamKine.toArray(new DatoStreamKinesis[listStreamKine.size()]);
       sb = new StringBuilder();
       for (int i=0 ; i<(listStreamKine.size()-1); i++) {
           sb.append("\"").append(listingArray[i].getActividad()).append("\";\"").append(listingArray[i].getBusqueda()).append("\";\"").append(listingArray[i].getCiudad()).append("\";\"")
                   .append(listingArray[i].getCp()).append("\";\"").append(listingArray[i].getDireccion()).append("\";").append(listingArray[i].getEdad()).append(";\"")
                   .append(listingArray[i].getEmail()).append("\";\"").append(listingArray[i].getDatafecha_loc()).append("\";\"").append(listingArray[i].getPoblacion()).append("\";\"")
                   .append(listingArray[i].getSexo()).append("\";").append("\n");

       }
       System.out.println("Valor SB:"+ sb.toString().trim());
       return sb.toString().trim();
    }

    /* * returns random integer between minimum and maximum range */
    private static int getRandomInteger(int maximum, int minimum){
        return ((int) (Math.random()*(maximum - minimum))) + minimum;
    }

    public static class DatoStreamKinesis{
        public String getCiudad() {
            return ciudad;
        }

        public void setCiudad(String ciudad) {
            this.ciudad = ciudad;
        }

        public String getPoblacion() {
            return poblacion;
        }

        public void setPoblacion(String poblacion) {
            this.poblacion = poblacion;
        }

        public String getDireccion() {
            return direccion;
        }

        public void setDireccion(String direccion) {
            this.direccion = direccion;
        }

        public String getSexo() {
            return sexo;
        }

        public void setSexo(String sexo) {
            this.sexo = sexo;
        }

        public String getActividad() {
            return actividad;
        }

        public void setActividad(String actividad) {
            this.actividad = actividad;
        }

        public String getBusqueda() {
            return busqueda;
        }

        public void setBusqueda(String busqueda) {
            this.busqueda = busqueda;
        }

        public String getDatafecha_loc() {
            return Datafecha_loc;
        }

        public void setDatafecha_loc(String datafecha_loc) {
            Datafecha_loc = "16/11/2020";
        }

        public int getEdad() {
            return edad;
        }

        public void setEdad(int edad) {
            this.edad = edad;
        }

        public String getEmail() {
            return email;
        }

        public void setEmail(String email) {
            this.email = email;
        }

        public String getCp() {
            return cp;
        }

        public void setCp(String cp) {
            this.cp = cp;
        }

        private String ciudad;
        private String poblacion;
        private String direccion;
        private String sexo;
        private String actividad;
        private String busqueda;
        private String Datafecha_loc= "26/11/2020";
        private int edad;
        private String email;
        private String cp;
    }
}
