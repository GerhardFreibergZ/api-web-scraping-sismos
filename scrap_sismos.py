import requests
from bs4 import BeautifulSoup
import boto3
import uuid

def lambda_handler(event, context):
    # URL de la página web con los sismos
    url = "https://ultimosismo.igp.gob.pe/ultimosismo/sismos-reportados"
    
    # Realizar la solicitud HTTP
    response = requests.get(url)
    if response.status_code != 200:
        return {
            'statusCode': response.status_code,
            'body': 'Error al acceder a la página web'
        }

    # Parsear el contenido HTML
    soup = BeautifulSoup(response.content, 'html.parser')

    # Encontrar la tabla de los sismos
    table = soup.find('table')
    if not table:
        return {
            'statusCode': 404,
            'body': 'No se encontró la tabla de sismos en la página'
        }

    # Extraer las filas de la tabla
    rows = []
    for row in table.find_all('tr')[1:11]:  # Obtener solo los primeros 10 sismos
        cells = row.find_all('td')
        if len(cells) > 0:
            sismo = {
                'id': str(uuid.uuid4()),  # ID único para cada sismo
                'referencia': cells[0].text.strip(),
                'ubicacion': cells[1].text.strip(),
                'fecha_hora': cells[2].text.strip(),
                'magnitud': cells[3].text.strip()
            }
            rows.append(sismo)

    # Guardar los datos en DynamoDB
    dynamodb = boto3.resource('dynamodb')
    table = dynamodb.Table('SismosReportados')

    # Eliminar los elementos anteriores
    scan = table.scan()
    with table.batch_writer() as batch:
        for each in scan['Items']:
            batch.delete_item(Key={'id': each['id']})

    # Insertar los nuevos sismos
    for row in rows:
        table.put_item(Item=row)

    # Retornar el resultado como JSON
    return {
        'statusCode': 200,
        'body': rows
    }
