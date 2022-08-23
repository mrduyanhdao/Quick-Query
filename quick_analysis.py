import os
import io
import yaml
import pandas as pd
from yaml.loader import SafeLoader
from google.cloud import bigquery
from jinja2 import Template

class filepath:
    def __init__(self,template, input, project_id,result,query_used ):
        self.template = template
        self.input = input
        self.project_id = project_id
        self.result = result
        self.query_used = query_used 
         
    def create_query(self):
        with open(self.input, 'r', encoding='UTF-8') as input:
            input = yaml.load(input, Loader=yaml.SafeLoader)
        with open(self.template, 'r', encoding='UTF-8') as file:
            query_raw =  file.read() 
            query_file = Template(query_raw)
            return  query_file.render(**input)
    def print_query(self):
        query = self.create_query()
        with open(self.query_used, "w") as f:
          print( query, file=f)
          
    def run_query(self):
        query = self.create_query()
        client = bigquery.Client(project= self.project_id)
        with client.query(query).to_dataframe() as df:
            df.to_excel(self.result)
if __name__ == "__main__":
    with open('Template/buildfile.yml', 'r') as buildfile:
        buildload =yaml.load(buildfile, Loader=yaml.SafeLoader)
    object = filepath( buildload['template'], buildload['input'], buildload['project_id'], buildload['result'], buildload['query'] )
    object.print_query()
    #object.run_query()
    

        


