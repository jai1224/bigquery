import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.internal.clients import bigquery
import datetime
from apache_beam.transforms import Create
from apache_beam.transforms import Map
import os
credential_path = "C:\\Users\\Jeyakar Mondreti\\venv\\tesla-stock-analysis-b1dbc26f972d.json"
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credential_path
#Input Table Specification
table_spec = bigquery.TableReference(
    projectId='tesla-stock-analysis',
    datasetId='tesla',
    tableId='tslv')

# Output Table specification
output_table_spec = 'tesla-stock-analysis:tesla.tslv_mean'
PROJECT_ID='tesla-stock-analysis'
#Schema of the Output table 
output_table_schema = {
    'fields': [{
        'name': 'Date', 'type': 'STRING', 'mode': 'NULLABLE'
    }, {
        'name': 'Close_Mean_value', 'type': 'FLOAT', 'mode': 'REQUIRED'
    }]
}

#bq_source = beam.io.BigQuerySource(table_spec=table_spec)

parser = argparse.ArgumentParser()
# parser.add_argument('--my-arg', help='description')
args, beam_args = parser.parse_known_args()

beam_options = PipelineOptions(
    beam_args,
    runner='DataflowRunner',
    #runner='DirectRunner'
    project='tesla-stock-analysis',
    job_name='tesla-stock-data-calc',
    temp_location='gs://tesla-stock-dataflow-temp-storage/',
    region='asia-south2')
    
def discard_incomplete(data):
    """Filters out records that don't have an information."""
    return len(str(data['Date'])) > 0 and len(str(data['Close'])) > 0
    
def del_unwanted_cols(data):
    """Deleting unwanted columns"""
    del data['High']
    del data['Low']
    del data['Volume']
    del data['Adj_Close']
    del data['Open']
    return data

# Create the Pipeline with the specified options.
with beam.Pipeline(options=beam_options) as p:
    #Read data from BigQuery
    #read_data = (p | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(table=table_spec) | 'Fetch Required Columns' >> beam.Map(lambda elem: elem['Close']) | beam.Map(print))
    read_data = (p | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(table=table_spec) #the output format is Pcollection of dictionary 
        | 'Fetch Required Columns' >> beam.Map(del_unwanted_cols) #delte the unwanted columns, keep date & Close
        | 'convert to Pcollection of Tuples' >> beam.Map(lambda x: (str(x['Date']), x['Close'])) #need to convert pcollection dict to tuple as Mean.
        | 'Get mean value per key' >> beam.combiners.Mean.PerKey() #Acual caluculation of Mean by grouping on Date column
        #| 'Format to dict' | beam.Map(lambda x: {"Date": x[0], "Mean_Close": x[1]})
        | 'Print Map Data' >> beam.Map(print)
        | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
           '{0}:tesla.tslv_mean'.format(PROJECT_ID),
           schema=output_table_schema,
           write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
           create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
           )
    )
        #| 'Delete incomplete records' >> beam.Filter(discard_incomplete)
        
    
    #Load transformed pcollection into output BigQuery table
    """read_data | beam.io.WriteToBigQuery(
        output_table_spec,
        schema=output_table_schema,
        write_disposition=beam.io.BigQueryDisposition.WRITE_TRUNCATE,
        create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)"""
        
        
    #read_data = (p | 'Read from BigQuery' >> beam.io.ReadFromBigQuery(table=table_spec) | 'Fetch Required Columns' >> beam.Map(list(inputDictionary.values()))
    #read_data = p | 'Read from BigQuery' >> beam.io.Read(bq_source)
        
    #Group the data by date
    #grouped_data = read_data | 'Group by column 1' >> beam.GroupByKey()

    #Calculate the mean of Column Close for each group
    #mean_data = grouped_data | 'Calculate Mean' >> beam.Map(lambda x: (x[0], sum(x[1]) / len(x[1])))
    
    #write the results to a text file
    #mean_data | 'write to Text'  >> beam.io.WriteToText('mean_value.txt')

print(read_data)



    