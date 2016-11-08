package data;

import java.io.Serializable;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;

public class DataFrameManager implements Serializable{
	/**
	 * 
	 */
	private static final long serialVersionUID = 2713820114331384573L;

	/**
	 * 
	 */
	

	public DataFrameManager()  {
		// TODO Auto-generated constructor stub
	}

	public JavaRDD<HeatMapDataEntry> datatordd(DataFrame inputdata)  {
	return inputdata.toJavaRDD().map(new Function<Row,HeatMapDataEntry>(){
		/**
		 * 
		 */
		private static final long serialVersionUID = 8924061687234120493L;

		/**
		 * 
		 */
		
		@Override
		public HeatMapDataEntry call(Row arg0) throws Exception {
			// TODO Auto-generated method stub
//			System.out.println("Started priting arguments *********************** ");
		  String finalstring = arg0.mkString(",");
//		  System.out.println("finalstring"+finalstring);
		  HeatMapDataEntry heatMapDataEntry =  new HeatMapDataEntry(finalstring.split(","));
		
			return heatMapDataEntry;
		}
	});
	}

}
