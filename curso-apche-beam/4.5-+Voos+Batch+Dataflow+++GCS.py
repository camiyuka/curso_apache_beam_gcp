# mandando dados para job da cloud storage como saída
# como entrada, pegando dados da cloud storage
import apache_beam as beam
import os
from apache_beam.options.pipeline_options import PipelineOptions # nova biblioteca importada

# dicionário de configuração de projeto
pipeline_options = {
    'project': 'dataflow-apache-beam-415812' ,
    'runner': 'DataflowRunner', # qual runner + Runner
    'region': 'southamerica-east1', # aonde vai rodar 
    'staging_location': 'gs://apache_dataflow-udemy/temp', # aonde vai ser a area de staging da pipeline
    'temp_location': 'gs://apache_dataflow-udemy/temp', # qual é a temp location
    'template_location': 'gs://apache_dataflow-udemy/template/batch_job_df_gcs_voos' } # aonde vai guardar o template que estou criando
     
pipeline_options = PipelineOptions.from_dictionary(pipeline_options) # ler as opções de um dicionário 
p1 = beam.Pipeline(options=pipeline_options)

serviceAccount = r"C:\\Users\\camil\Downloads\dataflow-apache-beam-415812-f900aff91ba8.json"
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = serviceAccount

class filtro(beam.DoFn):
  def process(self,record):
    if int(record[8]) > 0:
      return [record]

Tempo_Atrasos = (
  p1
  | "Importar Dados Atraso" >> beam.io.ReadFromText(r"gs://apache_dataflow-udemy/entrada/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Atraso" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com atraso" >> beam.ParDo(filtro())
  | "Criar par atraso" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Somar por key" >> beam.CombinePerKey(sum)
)

Qtd_Atrasos = (
  p1
  | "Importar Dados" >> beam.io.ReadFromText(r"gs://apache_dataflow-udemy/entrada/voos_sample.csv", skip_header_lines = 1)
  | "Separar por Vírgulas Qtd" >> beam.Map(lambda record: record.split(','))
  | "Pegar voos com Qtd" >> beam.ParDo(filtro())
  | "Criar par Qtd" >> beam.Map(lambda record: (record[4],int(record[8])))
  | "Contar por key" >> beam.combiners.Count.PerKey()
)

tabela_atrasos = (
    {'Qtd_Atrasos':Qtd_Atrasos,'Tempo_Atrasos':Tempo_Atrasos} 
    | beam.CoGroupByKey()
    | beam.io.WriteToText(r"gs://apache_dataflow-udemy/saída/Voos_atrasados_qtd.csv")
)

p1.run()