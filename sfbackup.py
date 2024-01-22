from simple_salesforce import Salesforce, SalesforceMalformedRequest, SalesforceResourceNotFound
from csv import DictWriter
from datetime import date
from pathlib import Path
from shutil import make_archive
import logging, os, shutil, boto3

# variaveis de ambiente
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_ACESS_SECRET_KEY = os.environ['AWS_ACESS_SECRET_KEY']
AWS_BUCKET_NAME = os.environ['AWS_BUCKET_NAME']
AWS_SERVICE_NAME = os.environ['AWS_SERVICE_NAME']
AWS_REGION_NAME = os.environ['AWS_REGION_NAME']
DATA_DIR = os.environ['DATA_DIR']
SF_USERNAME = os.environ['SF_USERNAME']
SF_PASSWORD = os.environ['SF_PASSWORD']
SF_CONSUMER_KEY = os.environ['SF_CONSUMER_KEY']
SF_CONSUMER_SECRET = os.environ['SF_CONSUMER_SECRET']
SF_DOMAIN = os.environ['SF_DOMAIN']
SF_OBJECTS_NAMES = os.environ['SF_OBJECTS_NAMES']
    
# login AWS S3 
client = boto3.client(
    service_name=AWS_SERVICE_NAME,
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_ACESS_SECRET_KEY,
    region_name=AWS_REGION_NAME
)

# login salesforce
sf = Salesforce(username=SF_USERNAME,password=SF_PASSWORD, consumer_key=SF_CONSUMER_KEY, consumer_secret=SF_CONSUMER_SECRET, domain=SF_DOMAIN)

# cria um subdiretório com a data de hoje
datapath = Path(DATA_DIR) / date.today().isoformat() 
try:
  datapath.mkdir(parents=True)
except FileExistsError:
  pass

# cria arquivo de logs
logging.basicConfig(filename = f'{datapath}' + ".txt", level = logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s", force=True)

# obtem a lista de objetos ​​que precisaremos fazer backup
if SF_OBJECTS_NAMES:
  names = SF_OBJECTS_NAMES
else:
  description = sf.describe()
  names = [obj['name'] for obj in description['sobjects'] if obj['queryable']]
countObj = len(names)

# logs de informações total de objetos encontrados
print(f"Total sObjects encontrados : {countObj}")
logging.info(f"Total sObjects encontrados : {countObj}")
print("------------------------------------------------------------------------------")
logging.info("------------------------------------------------------------------------------")

# para cada objeto precisaremos de todos os campos exportáveis.
for name in names:
  # Attachment não consumivel
  if name == 'Attachment':
    print(f"sObject {name} não encontrado para consulta...")
    logging.info(f"sObject {name} não encontrado para consulta...")
  else:    
    salesforceObject = sf.__getattr__(name) 
    # obtem uma lista dos campos do objeto.
    try:
      fieldNames = [field['name'] for field in salesforceObject.describe()['fields']]
    except SalesforceResourceNotFound:
      print(f"sObject {name} não encontrado para consulta...")
      logging.info(f"sObject {name} não encontrado para consulta...")
      pass
    # em seguida, cria uma consulta SOQL no objeto e faz um query_all
    try:
      results = sf.query_all("SELECT " + ", ".join(fieldNames) + " FROM " + name)['records']
    except SalesforceMalformedRequest as e:
      # ignora objetos com regras sobre como ser consultado. 
      continue
    # cria arquivos csv com registros dos objetos
    outputfile = datapath / (name+".csv")
    with outputfile.open(mode='w', encoding='utf_8_sig') as csvfile:
      writer = DictWriter(csvfile,fieldnames=fieldNames)
      writer.writeheader()
      for row in results:
        # cada linha tem uma chave de atributos que não queremos, remove.
        row.pop('attributes',None)
        writer.writerow(row)     
    # logs de informações total de registros por objeto
    total = len(results)
    print(f"sObject {name} total records :  {total}")
    logging.info(f"sObject {name} total records :  {total}")  
# logs de informações
print("------------------------------------------------------------------------------")
logging.info("------------------------------------------------------------------------------")

# Sumariza o tamanho dos arquivos
sizedir = 0
for ele in os.scandir(datapath): 
    sizedir+=os.stat(ele).st_size 
# logs de informações sumarizando o tamanho do backup
print("Backup " + date.today().isoformat() + f" finalizado com {sizedir} Bytes")
logging.info("Backup " + date.today().isoformat() + f" finalizado com {sizedir} Bytes")
# copia o arquivo para dentro do diretorio a ser comprimido
old_file = f'{datapath}' + '.txt'
shutil.copy(old_file, datapath)
# comprime o diretorio de data temporario
make_archive(date.today().isoformat(), 'zip', datapath)
# deleta o diretorio de data temporario
shutil.rmtree(datapath)
# encerra os logs 
logging.shutdown()
# remove o arquivo de logs temporario
os.remove(f'{datapath}' + '.txt')
# faz o upload do arquivo de backup para o s3
client.upload_file(f'{datapath}' + '.zip', f'{AWS_BUCKET_NAME}', date.today().isoformat() + '.zip')
# remove o arquivo de backup comprimido temporario
os.remove(f'{datapath}' + '.zip')