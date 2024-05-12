import json

import googlemaps

if __name__ == '__main__':
    key = "AIzaSyAzvlTi6GIV5hjkIsSEjT1tSBGYpvEbdws"

    # Use sua chave da API do Google Maps
    gmaps = googlemaps.Client(key=key)

    # Defina suas coordenadas
    origem = (-30.0690898, -51.211145)
    destino = (-30.0106249, -51.1227617)

    # Solicite direções via carros por padrão
    direcao_resultado = gmaps.distance_matrix(origem, destino)

    # A distância será uma string no formato 'X km' ou 'X m'
    print(f'A distância entre as coordenadas é {direcao_resultado.get('rows')[0].get('elements')[0].get('distance').get('text')}')