package main.java;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericArray;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import javax.security.auth.login.Configuration;
import java.io.*;
import java.util.Random;

/**
 * Created by sandy on 6/8/17.
 */
public class GMLStore {
    public String generateName(Random rnd, String characters, int length)
    {
        char[] name = new char[length];
        for (int i = 0; i < length; i++)
        {
            name[i] = characters.charAt(rnd.nextInt(characters.length()));
        }
        return StringUtils.capitalize(new String(name));
    }
    public void inputGenerator()throws IOException
    {
        StringBuilder sb=new StringBuilder("10\n");
        PrintWriter out=new PrintWriter("/home/sandy/Documents/GMLInput.txt");
        String characters="abcdefghijklmnopqrstuvwxyz";
        Random rnd=new Random();
        for(int t=0;t<10;t++)
        {
             sb.append(t+","+100000+"-"+2+"-"+99999+"-"+1+",");
             for(int v=1;v<=100000;v++)
             {
                 sb.append(v+",");
                 sb.append(generateName(rnd,characters,5)+"-"+(15+rnd.nextInt(70))+",");
             }
            for(int e=1;e<=99999;e++)
            {
                sb.append(e+",");
                sb.append(rnd.nextBoolean()+ "-" +rnd.nextBoolean()+((e==99999)?"":","));
            }
            sb.append("\n");
            out.write(sb.toString());
            out.flush();
            sb=new StringBuilder();
        }
        out.close();
    }
    public void parquetWriter() throws IOException,ClassNotFoundException {
        BufferedReader in=new BufferedReader(new FileReader("/home/sandy/Documents/GMLInput.txt"));
        Path path = new Path("/home/sandy/Documents/GMLschemadata.parquet");
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse(getClass().getResourceAsStream("RepeatingGMLschema.avsc"));
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path,schema,CompressionCodecName.SNAPPY, 10485760,ParquetWriter.DEFAULT_PAGE_SIZE);
        String line[],temp[];int v,e,t=Integer.parseInt(in.readLine());
        GenericRecord graph = new GenericData.Record(schema);
        GenericArray grapharray=new GenericData.Array(t,schema.getField("GraphArray").schema());
        GenericRecord time,vertex,edge,vertex_prop,edge_prop;
        GenericArray vertexarray,edgearray;
        for(int ts=0;ts<t;ts++)
        {
            line=in.readLine().split(",");
            time=new GenericData.Record(schema.getField("GraphArray").schema().getElementType());
            time.put("timestamp",Integer.parseInt(line[0]));
            temp=line[1].split("-");
            v=Integer.parseInt(temp[0]);
            e=Integer.parseInt(temp[2]);
            vertexarray=new GenericData.Array(v,schema.getField("GraphArray").schema().getElementType().getField("VertexArray").schema());
            for(int vert=2;vert<2+2*v;vert+=2)
            {
                 vertex=new GenericData.Record(schema.getField("GraphArray").schema().getElementType().getField("VertexArray").schema().getElementType());
                 vertex.put("vertex_id",Integer.parseInt(line[vert]));
                 vertex_prop=new GenericData.Record(schema.getField("GraphArray").schema().getElementType().getField("VertexArray").schema().getElementType().getField("VertexProperty").schema());
                 vertex_prop.put("name",line[vert+1].split("-")[0]);
                 vertex_prop.put("age",Integer.parseInt(line[vert+1].split("-")[1]));
                 vertex.put("VertexProperty",vertex_prop);
                 vertexarray.add(vertex);
            }
            edgearray=new GenericData.Array(e,schema.getField("GraphArray").schema().getElementType().getField("EdgeArray").schema());
            for(int ed=2+2*v;ed<2+2*v+2*e;ed+=2)
            {
                 edge=new GenericData.Record(schema.getField("GraphArray").schema().getElementType().getField("EdgeArray").schema().getElementType());
                 edge.put("edge_id",Integer.parseInt(line[ed]));
                 edge_prop=new GenericData.Record(schema.getField("GraphArray").schema().getElementType().getField("EdgeArray").schema().getElementType().getField("EdgeProperty").schema());
                 edge_prop.put("friend", Boolean.parseBoolean(line[ed+1].split("-")[0]));
                 edge_prop.put("follow", Boolean.parseBoolean(line[ed+1].split("-")[1]));
                 edge.put("EdgeProperty",edge_prop);
                 edgearray.add(edge);
            }
            time.put("VertexArray",vertexarray);
            time.put("EdgeArray",edgearray);
            grapharray.add(time);
        }
        graph.put("GraphArray",grapharray);
        writer.write(graph);
        writer.close();
    }
    public void parquetReader()throws IOException
    {
        AvroParquetReader<GenericRecord> reader=new AvroParquetReader<GenericRecord>(new Path("/home/sandy/Documents/GMLschemadata.parquet"));
        double startv,endv,startt,endt;
        PrintWriter outv=new PrintWriter("/home/sandy/Documents/PlotData/Vertices.csv"),outt=new PrintWriter("/home/sandy/Documents/PlotData/Time.csv");
        GenericRecord graph=reader.read();
        GenericArray grapharray=(GenericArray) graph.get("GraphArray");
        GenericRecord time,vertex,vertexproperty;
        GenericArray vertexarray;
        for(int t=0;t<grapharray.size();t++)
        {
            startt=System.currentTimeMillis();
            time=(GenericRecord)grapharray.get(t);
            vertexarray=(GenericArray)time.get("VertexArray");
            for(int v=0;v<vertexarray.size();v++)
            {
                startv=System.nanoTime();
                vertex=(GenericRecord)vertexarray.get(v);
                vertexproperty=(GenericRecord)vertex.get("VertexProperty");
                vertexproperty.get("name");
                endv=System.nanoTime();
                outv.write((endv-startv)+((v==vertexarray.size()-1)?"\n":","));
            }
            endt=System.currentTimeMillis();
            outt.write(t+","+(endt-startt)+"\n");
        }
        outv.close();
        outt.close();
        /*GenericRecord time=(GenericRecord)grapharray.get(30);
        GenericArray vertexarray=(GenericArray) time.get("VertexArray");
        GenericRecord vertex=(GenericRecord)vertexarray.get(1);
        GenericRecord vertexproperty=(GenericRecord)vertex.get("VertexProperty");
        System.out.println(vertexproperty.get("name")+","+vertexproperty.get("age"));
        for(int i=0;i<vertexarray.size();i++)
        {
            vertex=(GenericRecord)vertexarray.get(i);
            vertexproperty=(GenericRecord)vertex.get("VertexProperty");
            System.out.println(vertexproperty.get("name")+","+vertexproperty.get("age"));
        }*/
    }
    public static void main(String args[]) throws IOException,ClassNotFoundException {

        GMLStore obj = new GMLStore();
        obj.inputGenerator();
        obj.parquetWriter();
        obj.parquetReader();
    }
}
 /*     For non repeated nested records:

        time.put("timestamp", 0);
        GenericRecord vertex=new GenericData.Record(schema.getField("Vertex").schema());
        vertex.put("vertex_id", 1);
        GenericRecord vertex_prop=new GenericData.Record(schema.getField("Vertex").schema().getField("Vertex_Property").schema());
        vertex_prop.put("name", "Tom");
        vertex_prop.put("age",20);
        vertex.put("Vertex_Property",vertex_prop);
        GenericRecord edge=new GenericData.Record(schema.getField("Edge").schema());
        edge.put("edge_id",1);
        GenericRecord edge_prop=new GenericData.Record(schema.getField("Edge").schema().getField("Edge_Property").schema());
        edge_prop.put("friend",true);
        edge_prop.put("follow",false);
        edge.put("Edge_Property",edge_prop);
        time.put("Vertex",vertex);
        time.put("Edge",edge);
        Path path = new Path("/home/sandy/Documents/GMLschemadata.parquet");
        AvroParquetWriter<GenericRecord> writer = new AvroParquetWriter<GenericRecord>(path, schema);
        writer.write(time);
        time.put("timestamp",1);
        vertex.put("vertex_id",2);
        vertex_prop.put("name","Harry");
        vertex_prop.put("age",21);
        vertex.put("Vertex_Property",vertex_prop);
        edge.put("edge_id",2);
        edge_prop.put("friend",false);
        edge_prop.put("follow",true);
        edge.put("Edge_Property",edge_prop);
        time.put("Vertex",vertex);
        time.put("Edge",edge);*/
