import json
import math
import os

import duckdb
import googlemaps
import requests

import config

endpoint_pattern = "https://api.sos-rs.com/shelters?orderBy=prioritySum&order=desc&page={}&perPage={}"

def get_conn():
    return duckdb.connect("./storage/db/shelter.db")
def get_count_shelters() -> int:
    url = endpoint_pattern.format(1, 1)
    response = requests.get(url).json()
    return response.get("data").get("count")


def get_block_shelters(page, reg):
    print(f"Obtendo dados batch {page} ...")

    url = endpoint_pattern.format(page, reg)
    response = requests.get(url).json()

    if response.get("statusCode") != 200:
        return []

    return response.get("data").get("results")


def save_json_file(page, content):
    print(f"Salvando dados batch {page} ...")
    path = f"./storage/raw/shelter_part_{page}.json"

    # Remove caso arquivo já exista
    if os.path.exists(path):
        os.remove(path)

    with open(path, 'w') as file:
        file.write(json.dumps(content, indent=4))


def extract():
    count = get_count_shelters()
    batch_size = 100
    batch_counts = math.ceil(count / batch_size)

    print(f"Quantidade de batch's: {batch_counts}")

    for block in range(1, batch_counts+1):
        content = get_block_shelters(block, batch_size)
        save_json_file(block, content)
        print(f"Batch Concluido com sucesso!")


def transformAndLoad():
    query = """
        CREATE OR REPLACE TABLE shelters AS
        SELECT
            id, pix, capacity, petFriendly, 
            shelteredPeople, prioritySum, verified, 
            latitude, longitude, createdAt, updatedAt,
            
            unnest(shelterSupplies,  recursive := true),
            concat(address, ' - Rio Grande do Sul') AS address,
            UPPER(name) AS name_locale, 
            CASE
                WHEN contact LIKE '%whatsapp%' THEN contact
                WHEN contact IS NULL OR contact = '' THEN 'Não Informado'
                ELSE replace(replace(replace(replace(replace(contact, ' ', ''), '.', ''), '(', ''), ')', ''), '-', '')
            END AS contact
        FROM read_json_auto('./storage/raw/shelter_part_*.json', format = 'array');
    """

    print("Transformando e carregando dados no duckDB ... ")
    cursor = get_conn()
    cursor.execute(query)
    cursor.commit()


def joinResourceShelters():
    query_create_table = """
        CREATE OR REPLACE TABLE tb_s_p AS
        SELECT tb_s.name_locale s_locale, tb_s.latitude s_latitude, tb_s.longitude s_longitude,
                tb_p.name_locale p_locale, tb_p.latitude p_latitude, tb_p.longitude p_longitude,
                tb_s.RECURSO resource, NULL AS distance, '' AS distance_measure
        FROM (
            SELECT name_locale, name RECURSO, latitude, longitude
            FROM shelters 
            WHERE tags[1] = 'RemainingSupplies'
        ) tb_s LEFT JOIN (
            SELECT name_locale, name RECURSO, latitude, longitude
            FROM shelters 
            WHERE tags[1] = 'NeedDonations'
        ) tb_p ON tb_p.RECURSO = tb_s.RECURSO
        WHERE tb_p.name_locale IS NOT NULL;
    """

    # Criando base de cruzamento entre recursos sobressalentes e recursos em necessidade
    get_conn().execute(query_create_table)

def calculateDistance(s_latitude, s_longitude, p_latitude, p_longitude):
    key = config.API_KEY_GMAPS

    gmaps = googlemaps.Client(key=key)
    response = gmaps.distance_matrix((s_latitude, s_longitude), (p_latitude, p_longitude))
    response_parts = response.get('rows')[0].get('elements')[0].get('distance').get('text').split(" ")
    return response_parts[0], response_parts[1]

def getCoordenates(address):
    key = config.API_KEY_GMAPS

    gmaps = googlemaps.Client(key=key)
    geocode_result = gmaps.geocode(address)
    if geocode_result:
        location = geocode_result[0]['geometry']['location']
        return location['lat'], location['lng']
    else:
        return None, None

def enrichmentCoordenates():
    cursor = get_conn()

    query = """
        SELECT DISTINCT name_locale, address 
        FROM shelters 
        WHERE latitude IS NULL OR longitude IS NULL
    """

    query_update = """
        UPDATE shelters SET latitude = ?, longitude = ?
        WHERE name_locale = ? AND address = ?
    """

    for row in cursor.execute(query).fetchall():
        name_locale, address = row[0], row[1]
        lat, lng = getCoordenates(address)
        print(f"{name_locale} -> {lat} / {lng}")
        cursor.execute(query_update, (lat, lng, name_locale, address))



def enrichmentDistance():
    cursor = get_conn()

    query = """
        SELECT DISTINCT s_locale, s_latitude, s_longitude, p_locale, p_latitude, p_longitude FROM tb_s_p
    """

    query_update = """
        UPDATE tb_s_p SET distance = ?, distance_measure = ?
        WHERE s_locale = ? AND p_locale = ?
    """

    # Calcular e atualizar base com distância em KM dos abrigos
    for row in cursor.execute(query).fetchall():
        s_locale, s_latitude, s_longitude = row[0], row[1], row[2]
        p_locale, p_latitude, p_longitude = row[3], row[4], row[5]

        distance, distance_measure = calculateDistance(s_latitude, s_longitude, p_latitude, p_longitude)
        print(f"{s_locale} -> {p_locale} = {distance} {distance_measure}")
        cursor.execute(query_update, (distance, distance_measure, s_locale, p_locale))

def main():
    # Extrai Database Full
    # extract()
    # transformAndLoad()

    # Busca Latitude e Longitude de endereços não preenchidos
    enrichmentCoordenates()

    # Gera cruzamento de dados para report final
    joinResourceShelters()
    enrichmentDistance()

if __name__ == '__main__':
     main()